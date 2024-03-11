from sqlalchemy import and_, distinct, func, select

from orm.author import Author
from orm.shout import ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from services.logger import root_logger as logger


@query.field('get_topics_all')
def get_topics_all(_, _info):
    return get_with_stat(select(Topic))


@query.field('get_topics_by_community')
def get_topics_by_community(_, _info, community_id: int):
    q = select(Topic).where(Topic.community == community_id)
    return get_with_stat(q)


@query.field('get_topics_by_author')
def get_topics_by_author(_, _info, author_id=0, slug='', user=''):
    q = select(Topic)
    if author_id:
        q = q.join(Author).where(Author.id == author_id)
    elif slug:
        q = q.join(Author).where(Author.slug == slug)
    elif user:
        q = q.join(Author).where(Author.user == user)

    return get_with_stat(q)


@query.field('get_topic')
def get_topic(_, _info, slug: str):
    q = select(Topic).filter(Topic.slug == slug)
    topics = get_with_stat(q)
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


def topic_follow(follower_id, slug):
    try:
        with local_session() as session:
            topic = session.query(Topic).where(Topic.slug == slug).one()
            _following = TopicFollower(topic=topic.id, follower=follower_id)
        return None
    except Exception as exc:
        logger.error(exc)
        return exc


def topic_unfollow(follower_id, slug):
    try:
        with local_session() as session:
            sub = (
                session.query(TopicFollower)
                .join(Topic)
                .filter(and_(TopicFollower.follower == follower_id, Topic.slug == slug))
                .first()
            )
            if sub:
                session.delete(sub)
                session.commit()
        return None
    except Exception as ex:
        logger.debug(ex)
        return ex


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
