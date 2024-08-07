import json
import asyncio
from sqlalchemy import and_, join, select
from orm.author import Author, AuthorFollower
from orm.shout import Shout, ShoutAuthor, ShoutTopic, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from cache.cache import cache_author, cache_topic
from services.db import local_session
from utils.encoders import CustomJSONEncoder
from utils.logger import root_logger as logger
from services.redis import redis


# Предварительное кеширование подписчиков автора
async def precache_authors_followers(author_id, session):
    authors_followers = set()
    followers_query = select(AuthorFollower.follower).where(AuthorFollower.author == author_id)
    result = session.execute(followers_query)
    authors_followers.update(row[0] for row in result if row[0])

    followers_payload = json.dumps(list(authors_followers), cls=CustomJSONEncoder)
    await redis.execute("SET", f"author:followers:{author_id}", followers_payload)


# Предварительное кеширование подписок автора
async def precache_authors_follows(author_id, session):
    follows_topics_query = select(TopicFollower.topic).where(TopicFollower.follower == author_id)
    follows_authors_query = select(AuthorFollower.author).where(AuthorFollower.follower == author_id)
    follows_shouts_query = select(ShoutReactionsFollower.shout).where(ShoutReactionsFollower.follower == author_id)

    follows_topics = {row[0] for row in session.execute(follows_topics_query) if row[0]}
    follows_authors = {row[0] for row in session.execute(follows_authors_query) if row[0]}
    follows_shouts = {row[0] for row in session.execute(follows_shouts_query) if row[0]}

    topics_payload = json.dumps(list(follows_topics), cls=CustomJSONEncoder)
    authors_payload = json.dumps(list(follows_authors), cls=CustomJSONEncoder)
    shouts_payload = json.dumps(list(follows_shouts), cls=CustomJSONEncoder)

    await asyncio.gather(
        redis.execute("SET", f"author:follows-topics:{author_id}", topics_payload),
        redis.execute("SET", f"author:follows-authors:{author_id}", authors_payload),
        redis.execute("SET", f"author:follows-shouts:{author_id}", shouts_payload),
    )


# Предварительное кеширование авторов тем
async def precache_topics_authors(topic_id: int, session):
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
    topic_authors = {row[0] for row in session.execute(topic_authors_query) if row[0]}

    authors_payload = json.dumps(list(topic_authors), cls=CustomJSONEncoder)
    await redis.execute("SET", f"topic:authors:{topic_id}", authors_payload)


# Предварительное кеширование подписчиков тем
async def precache_topics_followers(topic_id: int, session):
    followers_query = select(TopicFollower.follower).where(TopicFollower.topic == topic_id)
    topic_followers = {row[0] for row in session.execute(followers_query) if row[0]}

    followers_payload = json.dumps(list(topic_followers), cls=CustomJSONEncoder)
    await redis.execute("SET", f"topic:followers:{topic_id}", followers_payload)


async def precache_data():
    try:
        # cache reset
        await redis.execute("FLUSHDB")
        logger.info("redis flushed")

        with local_session() as session:
            # topics
            q = select(Topic).where(Topic.community == 1)
            topics = get_with_stat(q)
            for topic in topics:
                topic_dict = topic.dict() if hasattr(topic, "dict") else topic
                await cache_topic(topic_dict)
                await asyncio.gather(
                    precache_topics_followers(topic["id"], session), precache_topics_authors(topic_dict["id"], session)
                )
            logger.info(f"{len(topics)} topics and their followings precached")

            # authors
            authors = get_with_stat(select(Author).where(Author.user.is_not(None)))
            logger.debug(f"{len(authors)} authors found in database")
            for author in authors:
                if isinstance(author, Author):
                    profile = author.dict()
                    author_id = profile.get("id")
                    user_id = profile.get("user", "").strip()
                    if author_id and user_id:
                        await cache_author(profile)
                        await asyncio.gather(
                            precache_authors_followers(author_id, session), precache_authors_follows(author_id, session)
                        )
                else:
                    logger.error(f"fail caching {author}")
            logger.info(f"{len(authors)} authors and their followings precached")
    except Exception as exc:
        import traceback

        traceback.print_exc()
        logger.error(f"Error in precache_data: {exc}")
