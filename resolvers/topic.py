from sqlalchemy import and_, distinct, func, select
from sqlalchemy.orm import aliased

from orm.author import Author
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query


async def followed_topics(follower_id):
    q = select(Author)
    q = add_topic_stat_columns(q)
    q = q.join(TopicFollower, TopicFollower.author == Author.id).where(TopicFollower.follower == follower_id)
    # Pass the query to the get_authors_from_query function and return the results
    return get_topics_from_query(q)


def add_topic_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_topic_follower = aliased(TopicFollower)

    q = (
        q.outerjoin(ShoutTopic, Topic.id == ShoutTopic.topic)
        .add_columns(func.count(distinct(ShoutTopic.shout)).label("shouts_stat"))
        .outerjoin(aliased_shout_author, ShoutTopic.shout == aliased_shout_author.shout)
        .add_columns(func.count(distinct(aliased_shout_author.author)).label("authors_stat"))
        .outerjoin(aliased_topic_follower)
        .add_columns(func.count(distinct(aliased_topic_follower.follower)).label("followers_stat"))
    )

    q = q.group_by(Topic.id)

    return q


def get_topics_from_query(q):
    topics = []
    with local_session() as session:
        for [topic, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            topic.stat = {
                "shouts": shouts_stat,
                "authors": authors_stat,
                "followers": followers_stat,
            }
            topics.append(topic)

    return topics


@query.field("get_topics_all")
async def get_topics_all(_, _info):
    q = select(Topic)
    q = add_topic_stat_columns(q)

    return get_topics_from_query(q)


def topics_followed_by(author_id):
    q = select(Topic)
    q = add_topic_stat_columns(q)
    q = q.join(TopicFollower).where(TopicFollower.follower == author_id)

    return get_topics_from_query(q)


@query.field("get_topics_by_community")
async def get_topics_by_community(_, _info, community_id: int):
    q = select(Topic).where(Topic.community == community_id)
    q = add_topic_stat_columns(q)

    return get_topics_from_query(q)


@query.field("get_topics_by_author")
async def get_topics_by_author(_, _info, author_id=None, slug="", user=""):
    q = select(Topic)
    q = add_topic_stat_columns(q)
    if author_id:
        q = q.join(Author).where(Author.id == author_id)
    elif slug:
        q = q.join(Author).where(Author.slug == slug)
    elif user:
        q = q.join(Author).where(Author.user == user)

    return get_topics_from_query(q)


@query.field("get_topic")
async def get_topic(_, _info, slug):
    q = select(Topic).where(Topic.slug == slug)
    q = add_topic_stat_columns(q)

    topics = get_topics_from_query(q)
    return topics[0]


@mutation.field("create_topic")
@login_required
async def create_topic(_, _info, inp):
    with local_session() as session:
        # TODO: check user permissions to create topic for exact community
        new_topic = Topic(**inp)
        session.add(new_topic)
        session.commit()

    return {"topic": new_topic}


@mutation.field("update_topic")
@login_required
async def update_topic(_, _info, inp):
    slug = inp["slug"]
    with local_session() as session:
        topic = session.query(Topic).filter(Topic.slug == slug).first()
        if not topic:
            return {"error": "topic not found"}
        else:
            Topic.update(topic, inp)
            session.add(topic)
            session.commit()

            return {"topic": topic}


@mutation.field("delete_topic")
@login_required
async def delete_topic(_, info, slug: str):
    user_id = info.context["user_id"]
    with local_session() as session:
        t: Topic = session.query(Topic).filter(Topic.slug == slug).first()
        if not t:
            return {"error": "invalid topic slug"}
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            if t.created_by != author.id:
                return {"error": "access denied"}

            session.delete(t)
            session.commit()

            return {}
        else:
            return {"error": "access denied"}


def topic_follow(follower_id, slug):
    try:
        with local_session() as session:
            topic = session.query(Topic).where(Topic.slug == slug).one()
            _following = TopicFollower(topic=topic.id, follower=follower_id)
            return True
    except Exception:
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
    except Exception:
        pass
    return False


@query.field("get_topics_random")
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
