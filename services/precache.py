import json

from sqlalchemy import and_, join, select

from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.rediscache import redis
from resolvers.stat import (
    get_with_stat,
    get_author_shouts_stat,
    get_author_comments_stat,
)


async def precache_data():
    # Удаляем все кэшированные данные
    await redis.execute("FLUSHDB")

    authors_by_id = {}
    topics_by_id = {}
    # authors precache
    logger.info("Precaching authors")
    authors = get_with_stat(select(Author))
    for a in authors:
        profile = a.dict() if not isinstance(a, dict) else a
        author_id = profile.get("id")
        if author_id:
            authors_by_id[author_id] = profile
            await redis.execute("SET", f"author:{author_id}", json.dumps(profile, cls=CustomJSONEncoder))
            await redis.execute(
                "SET",
                f"user:{profile['user']}",
                json.dumps(profile, cls=CustomJSONEncoder),
            )

    # topics precache
    logger.info("Precaching topics")
    topics = get_with_stat(select(Topic))
    for t in topics:
        topic = t.dict() if not isinstance(t, dict) else t
        topic_id = topic.get("id")
        topics_by_id[topic_id] = topic
        await redis.execute("SET", f"topic:{topic_id}", json.dumps(topic, cls=CustomJSONEncoder))

    authors_keys = authors_by_id.keys()
    logger.info("Precaching following data")
    for author_id in authors_keys:
        with local_session() as session:
            # follows topics precache
            follows_topics = set()
            follows_topics_query = (
                select(Topic.id)
                .select_from(join(Topic, TopicFollower, Topic.id == TopicFollower.topic))
                .where(TopicFollower.follower == author_id)
            )
            for followed_topic_id in session.execute(follows_topics_query):
                ft = topics_by_id.get(followed_topic_id)
                if ft:
                    follows_topics.add(ft)

            # follows authors precache
            follows_authors = set()
            follows_authors_query = (
                select(Author.id)
                .select_from(
                    join(
                        Author,
                        AuthorFollower,
                        Author.id == AuthorFollower.author,
                    )
                )
                .where(AuthorFollower.follower == author_id)
            )
            for followed_author_id in session.execute(follows_authors_query):
                followed_author = authors_by_id.get(followed_author_id)
                if followed_author:
                    follows_authors.add(followed_author)

            # followers precache
            followers = set()
            followers_query = select(Author.id).join(
                AuthorFollower,
                and_(
                    AuthorFollower.author == author_id,
                    AuthorFollower.follower == Author.id,
                ),
            )
            for follower_id in session.execute(followers_query):
                follower = authors_by_id.get(follower_id)
                if follower:
                    followers.add(follower)

            # shouts and comments precache
            shouts_stat = get_author_shouts_stat(author_id)
            comments_stat = get_author_comments_stat(author_id)

            authors_payload = json.dumps(
                [f.dict() if isinstance(f, Author) else f for f in follows_authors],
                cls=CustomJSONEncoder,
            )
            await redis.execute("SET", f"author:{author_id}:follows-authors", authors_payload)
            topics_payload = json.dumps(
                [t.dict() if isinstance(t, Topic) else t for t in follows_topics],
                cls=CustomJSONEncoder,
            )
            await redis.execute("SET", f"author:{author_id}:follows-topics", topics_payload)
            followers_payload = json.dumps(
                [f.dict() if isinstance(f, Author) else f for f in followers],
                cls=CustomJSONEncoder,
            )
            await redis.execute("SET", f"author:{author_id}:followers", followers_payload)
            await redis.execute("SET", f"author:{author_id}:shouts-stat", shouts_stat)
            await redis.execute("SET", f"author:{author_id}:comments-stat", comments_stat)
    logger.info(f"{len(authors)} authors were precached")
