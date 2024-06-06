import json

from sqlalchemy import and_, join, select

from orm.author import Author, AuthorFollower
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.cache import cache_author, cache_topic
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.rediscache import redis


async def precache_authors_followers(author_id, session):
    # Precache author followers
    authors_followers = set()
    followers_query = select(AuthorFollower.follower).where(AuthorFollower.author == author_id)
    result = session.execute(followers_query)

    for row in result:
        follower_id = row[0]
        if follower_id:
            authors_followers.add(follower_id)

    followers_payload = json.dumps(
        [f for f in authors_followers],
        cls=CustomJSONEncoder,
    )
    await redis.execute("SET", f"author:followers:{author_id}", followers_payload)


async def precache_authors_follows(author_id, session):
    # Precache topics followed by author
    follows_topics = set()
    follows_topics_query = select(TopicFollower.topic).where(TopicFollower.follower == author_id)
    result = session.execute(follows_topics_query)

    for row in result:
        followed_topic_id = row[0]
        if followed_topic_id:
            follows_topics.add(followed_topic_id)

    topics_payload = json.dumps([t for t in follows_topics], cls=CustomJSONEncoder)
    await redis.execute("SET", f"author:follows-topics:{author_id}", topics_payload)

    # Precache authors followed by author
    follows_authors = set()
    follows_authors_query = select(AuthorFollower.author).where(AuthorFollower.follower == author_id)
    result = session.execute(follows_authors_query)

    for row in result:
        followed_author_id = row[0]
        if followed_author_id:
            follows_authors.add(followed_author_id)

    authors_payload = json.dumps([a for a in follows_authors], cls=CustomJSONEncoder)
    await redis.execute("SET", f"author:follows-authors:{author_id}", authors_payload)


async def precache_topics_authors(topic_id: int, session):
    # Precache topic authors
    topic_authors = set()
    topic_authors_query = (
        select(ShoutAuthor.author)
        .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
        .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
        .filter(
            and_(
                ShoutTopic.topic == topic_id,
                Shout.published_at.is_not(None),
                Shout.deleted_at.is_(None),
            )
        )
    )
    result = session.execute(topic_authors_query)

    for row in result:
        author_id = row[0]
        if author_id:
            topic_authors.add(author_id)

    authors_payload = json.dumps([a for a in topic_authors], cls=CustomJSONEncoder)
    await redis.execute("SET", f"topic:authors:{topic_id}", authors_payload)


async def precache_topics_followers(topic_id: int, session):
    # Precache topic followers
    topic_followers = set()
    followers_query = select(TopicFollower.follower).where(TopicFollower.topic == topic_id)
    result = session.execute(followers_query)

    for row in result:
        follower_id = row[0]
        if follower_id:
            topic_followers.add(follower_id)

    followers_payload = json.dumps([f for f in topic_followers], cls=CustomJSONEncoder)
    await redis.execute("SET", f"topic:followers:{topic_id}", followers_payload)


async def precache_data():
    try:
        # cache reset
        await redis.execute("FLUSHDB")
        logger.info("redis flushed")

        # topics
        topics_by_id = {}
        topics = get_with_stat(select(Topic))
        for topic in topics:
            topic_profile = topic.dict() if not isinstance(topic, dict) else topic
            await cache_topic(topic_profile)
        logger.info(f"{len(topics)} topics precached")

        # followings for topics
        with local_session() as session:
            for topic_id in topics_by_id.keys():
                await precache_topics_followers(topic_id, session)
                await precache_topics_authors(topic_id, session)
        logger.info("topics followings precached")

        # authors
        authors_by_id = {}
        authors = get_with_stat(select(Author).where(Author.user.is_not(None)))
        logger.debug(f"{len(authors)} authors connected with authorizer")
        for author in authors:
            profile = author.dict() if not isinstance(author, dict) else author
            author_id = profile.get("id")
            user_id = profile.get("user", "").strip()
            if user_id == "FyPGkAwnrXPiv2PxQ":
                logger.warning(profile)
            if author_id and user_id:
                authors_by_id[author_id] = profile
                await cache_author(profile)
            else:
                logger.error(f"fail caching {author.dict()}")
        logger.info(f"{len(authors)} authors precached")

        # followings for authors
        with local_session() as session:
            for author_id in authors_by_id.keys():
                await precache_authors_followers(author_id, session)
                await precache_authors_follows(author_id, session)
        logger.info("authors followings precached")
    except Exception as exc:
        logger.error(exc)
