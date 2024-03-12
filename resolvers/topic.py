from sqlalchemy import distinct, func, select

from orm.author import Author
from orm.shout import ShoutTopic
from orm.topic import Topic
from resolvers.stat import get_topics_with_stat_cached
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query


@query.field('get_topics_all')
async def get_topics_all(_, _info):
    return await get_topics_with_stat_cached(select(Topic))


@query.field('get_topics_by_community')
async def get_topics_by_community(_, _info, community_id: int):
    q = select(Topic).where(Topic.community == community_id)
    return await get_topics_with_stat_cached(q)


@query.field('get_topics_by_author')
async def get_topics_by_author(_, _info, author_id=0, slug='', user=''):
    q = select(Topic)
    if author_id:
        q = q.join(Author).where(Author.id == author_id)
    elif slug:
        q = q.join(Author).where(Author.slug == slug)
    elif user:
        q = q.join(Author).where(Author.user == user)

    return await get_topics_with_stat_cached(q)


@query.field('get_topic')
async def get_topic(_, _info, slug: str):
    q = select(Topic).filter(Topic.slug == slug)
    topics = await get_topics_with_stat_cached(q)
    if topics:
        return topics[0]


@mutation.field('create_topic')
@login_required
async def create_topic(_, _info, inp):
    with local_session() as session:
        # TODO: check user permissions to create topic for exact community
        # and actor is permitted to craete it
        new_topic = Topic(**inp)
        session.add(new_topic)
        session.commit()

        return {'topic': new_topic}


@mutation.field('update_topic')
@login_required
async def update_topic(_, _info, inp):
    slug = inp['slug']
    with local_session() as session:
        topic = session.query(Topic).filter(Topic.slug == slug).first()
        if not topic:
            return {'error': 'topic not found'}
        else:
            Topic.update(topic, inp)
            session.add(topic)
            session.commit()

            return {'topic': topic}


@mutation.field('delete_topic')
@login_required
async def delete_topic(_, info, slug: str):
    user_id = info.context['user_id']
    with local_session() as session:
        t: Topic = session.query(Topic).filter(Topic.slug == slug).first()
        if not t:
            return {'error': 'invalid topic slug'}
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            if t.created_by != author.id:
                return {'error': 'access denied'}

            session.delete(t)
            session.commit()

            return {}
    return {'error': 'access denied'}


@query.field('get_topics_random')
def get_topics_random(_, _info, amount=12):
    q = select(Topic)
    q = q.join(ShoutTopic)
    q = q.group_by(Topic.id)
    q = q.having(func.count(distinct(ShoutTopic.shout)) > 2)
    q = q.order_by(func.random()).limit(amount)

    topics = []
    with local_session() as session:
        for [topic] in session.execute(q):
            topics.append(topic)

    return topics
