import json
from typing import List

from sqlalchemy import and_, join, select

from orm.author import Author, AuthorFollower
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.rediscache import redis

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


async def cache_topic(topic: dict):
    topic_id = topic.get("id")
    topic_slug = topic.get("slug")
    payload = json.dumps(topic, cls=CustomJSONEncoder)
    await redis.execute("SET", f"topic:id:{topic_id}", payload)
    await redis.execute("SET", f"topic:slug:{topic_slug}", topic_id)


async def cache_author(author: dict):
    author_id = author.get("id")
    user_id = author.get("user")
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute("SET", f"author:user:{user_id}", author_id)
    await redis.execute("SET", f"author:id:{author_id}", payload)


async def cache_follows(follower_id: int, entity_type: str, entity_id: int, is_insert=True):
    follows = []
    redis_key = f"author:follows-{entity_type}s:{follower_id}"
    follows_str = await redis.execute("GET", redis_key)
    if isinstance(follows_str, str):
        follows = json.loads(follows_str)
    if is_insert:
        if entity_id not in follows:
            follows.append(entity_id)
    else:
        if not entity_id:
            raise Exception("wrong entity")
        follows = [eid for eid in follows if eid != entity_id]

    payload = json.dumps(follows, cls=CustomJSONEncoder)
    await redis.execute("SET", redis_key, payload)

    follower_str = await redis.execute("GET", f"author:id:{follower_id}")
    if isinstance(follower_str, str):
        follower = json.loads(follower_str)
        follower["stat"][f"{entity_type}s"] = len(follows)
        await cache_author(follower)


async def get_cached_author(author_id: int, get_with_stat):
    if author_id:
        rkey = f"author:id:{author_id}"
        cached_result = await redis.execute("GET", rkey)
        if isinstance(cached_result, str):
            return json.loads(cached_result)
        elif get_with_stat:
            with local_session() as session:
                author_query = select(Author).filter(Author.id == author_id)
                result = get_with_stat(session.execute(author_query))
                if result:
                    [author] = result
                    if author:
                        await cache_author(author.dict())
                        return author


async def get_cached_author_by_user_id(user_id: str, get_with_stat) -> dict:
    author_str = await redis.execute("GET", f"author:user:{user_id}")
    author_dict = None
    if not author_str:
        author_query = select(Author).filter(Author.user == user_id)
        [author_with_stat] = get_with_stat(author_query)
        if author_with_stat:
            await cache_author(author_with_stat.dict())
            author_dict = author_with_stat.dict()
    else:
        author_dict = json.loads(author_str)
    return author_dict


async def get_cached_topic_by_slug(slug: str, get_with_stat):
    cached_result = await redis.execute("GET", f"topic:slug:{slug}")
    if isinstance(cached_result, str):
        return json.loads(cached_result)
    elif get_with_stat:
        with local_session() as session:
            topic_query = select(Topic).filter(Topic.slug == slug)
            result = get_with_stat(session.execute(topic_query))
            if result:
                [topic] = result
                if topic:
                    await cache_topic(topic)
                    return topic


async def get_cached_authors_by_ids(authors_ids: List[int]) -> List[Author | dict]:
    authors = []
    for author_id in authors_ids:
        if author_id:
            rkey = f"author:id:{author_id}"
            cached_result = await redis.execute("GET", rkey)
            if isinstance(cached_result, str):
                author = json.loads(cached_result)
                if author:
                    authors.append(author)
    return authors


async def get_cached_topic_authors(topic_id: int):
    rkey = f"topic:authors:{topic_id}"
    cached = await redis.execute("GET", rkey)
    authors_ids = []
    if isinstance(cached, str):
        authors_ids = json.loads(cached)
    else:
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
        with local_session() as session:
            authors_ids = [aid for (aid,) in session.execute(topic_authors_query)]
    await redis.execute("SET", rkey, json.dumps(authors_ids))
    authors = await get_cached_authors_by_ids(authors_ids)
    logger.debug(f"topic#{topic_id} cache updated with {len(authors)} authors")
    return authors


async def get_cached_topic_followers(topic_id: int):
    followers = []
    rkey = f"topic:followers:{topic_id}"
    cached = await redis.execute("GET", rkey)
    if isinstance(cached, str):
        followers = json.loads(cached)
        if isinstance(followers, list):
            return followers
    with local_session() as session:
        result = (
            session.query(Author.id)
            .join(
                TopicFollower,
                and_(TopicFollower.topic == topic_id, TopicFollower.follower == Author.id),
            )
            .all()
        )
        followers_ids = [f[0] for f in result]
    followers = await get_cached_authors_by_ids(followers_ids)
    logger.debug(f"topic#{topic_id} cache updated with {len(followers)} followers")
    return followers


async def get_cached_author_followers(author_id: int):
    # follower profile
    cached_author = await redis.execute("GET", f"author:id:{author_id}")
    author = None
    if cache_author:
        author = json.loads(cached_author)
    if not author:
        return []

    followers_ids = []
    followers_rkey = f"author:followers:{author_id}"
    cached = await redis.execute("GET", followers_rkey)
    if isinstance(cached, str) and isinstance(cached_author, str):
        followers_ids = json.loads(cached) or []
        if not str(len(followers_ids)) == str(author["stat"]["followers"]):
            with local_session() as session:
                followers_result = (
                    session.query(Author.id)
                    .join(
                        AuthorFollower,
                        and_(
                            AuthorFollower.author == author_id,
                            AuthorFollower.follower == Author.id,
                            Author.id != author_id,  # exclude the author from the followers
                        ),
                    )
                    .all()
                )
                followers_ids = [a[0] for a in followers_result]
                await redis.execute("SET", followers_rkey, json.dumps(followers_ids))
        else:
            followers = await get_cached_authors_by_ids(followers_ids)

    logger.debug(f"author#{author_id} cache updated with {len(followers)} followers")
    return followers


async def get_cached_follower_authors(author_id: int):
    rkey = f"author:follows-authors:{author_id}"
    authors_ids = []
    cached = await redis.execute("GET", rkey)
    if not cached:
        authors_query = (
            select(Author.id)
            .select_from(join(Author, AuthorFollower, Author.id == AuthorFollower.author))
            .where(AuthorFollower.follower == author_id)
        )
        with local_session() as session:
            result = session.execute(authors_query)
            authors_ids = [a[0] for a in result]
            await redis.execute("SET", rkey, json.dumps(authors_ids))
    elif isinstance(cached, str):
        authors_ids = json.loads(cached)
    return await get_cached_authors_by_ids(authors_ids)


async def get_cached_follower_topics(author_id: int):
    rkey = f"author:follows-topics:{author_id}"
    topics_ids = []
    cached = await redis.execute("GET", rkey)
    if cached and isinstance(cached, str):
        topics_ids = json.loads(cached)
    else:
        with local_session() as session:
            topics = (
                session.query(Topic)
                .select_from(join(Topic, TopicFollower, Topic.id == TopicFollower.topic))
                .where(TopicFollower.follower == author_id)
                .all()
            )

            topics_ids = [topic.id for topic in topics]

    await redis.execute("SET", rkey, json.dumps(topics_ids))

    topics = []
    for topic_id in topics_ids:
        topic_str = await redis.execute("GET", f"topic:id:{topic_id}")
        if topic_str:
            topic = json.loads(topic_str)
            if topic and topic not in topics:
                topics.append(topic)

    logger.debug(f"author#{author_id} cache updated with {len(topics)} topics")
    return topics
