from sqlalchemy import and_, distinct, func, select
from sqlalchemy.orm import aliased

from orm.author import Author
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from services.viewed import ViewedStorage
from services.logger import root_logger as logger


def add_topic_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_topic_follower = aliased(TopicFollower)

    q = (
        q.outerjoin(ShoutTopic, Topic.id == ShoutTopic.topic)
        .add_columns(func.count(distinct(ShoutTopic.shout)).label('shouts_stat'))
        .outerjoin(aliased_shout_author, ShoutTopic.shout == aliased_shout_author.shout)
        .add_columns(
            func.count(distinct(aliased_shout_author.author)).label('authors_stat')
        )
        .outerjoin(aliased_topic_follower)
        .add_columns(
            func.count(distinct(aliased_topic_follower.follower)).label(
                'followers_stat'
            )
        )
    )

    q = q.group_by(Topic.id)

    return q


async def get_topics_from_query(q):
    topics = []
    with local_session() as session:
        for [topic, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            topic.stat = {
                'shouts': shouts_stat,
                'authors': authors_stat,
                'followers': followers_stat,
                'viewed': await ViewedStorage.get_topic(topic.slug),
            }
            topics.append(topic)

    return topics


@query.field('get_topics_all')
async def get_topics_all(_, _info):
    q = select(Topic)
    q = add_topic_stat_columns(q)

    return await get_topics_from_query(q)


async def topics_followed_by(author_id):
    q = select(Topic, TopicFollower)
    q = add_topic_stat_columns(q)
    q = q.join(TopicFollower).where(TopicFollower.follower == author_id)

    return await get_topics_from_query(q)


@query.field('get_topics_by_community')
async def get_topics_by_community(_, _info, community_id: int):
    q = select(Topic).where(Topic.community == community_id)
    q = add_topic_stat_columns(q)

    return await get_topics_from_query(q)


@query.field('get_topics_by_author')
async def get_topics_by_author(_, _info, author_id=None, slug='', user=''):
    q = select(Topic)
    q = add_topic_stat_columns(q)
    if author_id:
        q = q.join(Author).where(Author.id == author_id)
    elif slug:
        q = q.join(Author).where(Author.slug == slug)
    elif user:
        q = q.join(Author).where(Author.user == user)

    return await get_topics_from_query(q)


@query.field('get_topic')
async def get_topic(_, _info, slug):
    q = select(Topic).where(Topic.slug == slug)
    q = add_topic_stat_columns(q)

    topics = await get_topics_from_query(q)
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
            return True
    except Exception as _exc:
        return False


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
                return True
    except Exception as ex:
        logger.debug(ex)
    return False


@query.field('get_topics_random')
async def get_topics_random(_, info, amount=12):
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


def get_random_topic():
    q = select(Topic)
    q = q.join(ShoutTopic)
    q = q.group_by(Topic.id)
    q = q.having(func.count(distinct(ShoutTopic.shout)) > 10)
    q = q.order_by(func.random()).limit(1)

    with local_session() as session:
        r = session.execute(q).first()
        if r:
            [topic] = r
            return topic
    return
