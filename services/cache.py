import json

from sqlalchemy import and_, join, select

from orm.author import Author, AuthorFollower
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
    payload = json.dumps(topic.dict(), cls=CustomJSONEncoder)
    await redis.execute("SET", f"topic:id:{topic_id}", payload)
    await redis.execute("SET", f"topic:slug:{topic_slug}", topic.id)


async def cache_author(author: dict):
    author_id = author.get("id")
    user_id = author.get("user")
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute("SET", f"user:id:{user_id}", author_id)
    await redis.execute("SET", f"author:id:{author_id}", payload)


async def cache_follows(
    follower_id: int, entity_type: str, entity_id: int, is_insert=True
):
    # prepare
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
        # Remove the entity from follows
        follows = [eid for eid in follows if eid != entity_id]

    # update follows cache
    payload = json.dumps(follows, cls=CustomJSONEncoder)
    await redis.execute("SET", redis_key, payload)

    # update follower's stats everywhere
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
        else:
            author_query = select(Author).filter(Author.id == author_id)
            [author] = get_with_stat(author_query)
            if author:
                await cache_author(author.dict())


async def get_cached_author_by_user_id(user_id: str, get_with_stat):
    author_id = await redis.execute("GET", f"user:id:{user_id}")
    if author_id:
        return await get_cached_author(int(author_id), get_with_stat)


async def get_cached_author_follows_topics(author_id: int):
    topics = []
    rkey = f"author:follows-topics:{author_id}"
    cached = await redis.execute("GET", rkey)
    if cached and isinstance(cached, str):
        topics = json.loads(cached)
    if not cached:
        topics = (
            local_session()
            .query(Topic.id)
            .select_from(join(Topic, TopicFollower, Topic.id == TopicFollower.topic))
            .where(TopicFollower.follower == author_id)
            .all()
        )
        await redis.execute("SET", rkey, json.dumps(topics))

        topics_objects = []
        for topic_id in topics:
            topic_str = await redis.execute("GET", f"topic:id:{topic_id}")
            if topic_str:
                topic = json.loads(topic_str)
                if topic and topic not in topics_objects:
                    topics_objects.append(topic)
        logger.debug(
            f"author#{author_id} cache updated with {len(topics_objects)} topics"
        )
        return topics_objects


async def get_cached_author_follows_authors(author_id: int):
    authors = []
    rkey = f"author:follows-authors:{author_id}"
    cached = await redis.execute("GET", rkey)
    if not cached:
        authors_query = (
            select(Author.id)
            .select_from(
                join(Author, AuthorFollower, Author.id == AuthorFollower.author)
            )
            .where(AuthorFollower.follower == author_id)
            .all()
        )
        authors = local_session().execute(authors_query)
        await redis.execute("SET", rkey, json.dumps([aid for aid in authors]))
    elif isinstance(cached, str):
        authors = json.loads(cached)
    authors_objects = []
    for author_id in authors:
        author_str = await redis.execute("GET", f"author:id:{author_id}")
        if author_str:
            author = json.loads(author_str)
            if author and author not in authors_objects:
                authors_objects.append(author)
    return authors_objects


async def get_cached_author_followers(author_id: int):
    followers = []
    followers_rkey = f"author:followers:{author_id}"
    cached = await redis.execute("GET", followers_rkey)
    cached_author = await redis.execute("GET", f"author:followers:{author_id}")
    if isinstance(cached, str) and isinstance(cached_author, str):
        followers = json.loads(cached)
        author = json.loads(cache_author)
        if isinstance(followers, list) and str(len(followers)) == str(
            author["stat"]["followers"]
        ):
            return followers

    followers = (
        local_session()
        .query(Author)
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

    await redis.execute("SET", followers_rkey, json.dumps([a.id for a in followers]))

    followers_objects = []
    for follower_id in followers:
        follower_str = await redis.execute("GET", f"author:id:{follower_id}")
        if follower_str:
            follower = json.loads(follower_str)
            if follower and follower not in followers_objects:
                followers_objects.append(follower)
    logger.debug(f"author#{author_id} cache updated with {len(followers)} followers")
    return followers_objects


async def get_cached_topic_followers(topic_id: int):
    followers = []
    rkey = f"topic:followers:{topic_id}"
    cached = await redis.execute("GET", rkey)
    if isinstance(cached, str):
        followers = json.loads(cached)
        if isinstance(followers, list):
            return followers

    followers = (
        local_session()
        .query(Author)
        .join(
            TopicFollower,
            and_(TopicFollower.topic == topic_id, TopicFollower.follower == Author.id),
        )
        .all()
    )
    followers_objects = []
    if followers:
        await redis.execute("SET", rkey, json.dumps([a.id for a in followers]))

        for follower_id in followers:
            follower_str = await redis.execute("GET", f"author:id:{follower_id}")
            if follower_str:
                follower = json.loads(follower_str)
                if follower and follower not in followers_objects:
                    followers_objects.append(follower)
        logger.debug(f"topic#{topic_id} cache updated with {len(followers)} followers")
    return followers_objects


async def get_cached_topic_authors(topic_id: int, topic_authors_query):
    authors = []
    rkey = f"topic:authors:{topic_id}"
    cached = await redis.execute("GET", rkey)
    if isinstance(cached, str):
        authors = json.loads(cached)
        if isinstance(authors, list):
            return authors

    authors = local_session().execute(topic_authors_query)
    authors_objects = []
    if authors:
        await redis.execute("SET", rkey, json.dumps(authors))
        for author_id in authors:
            author_str = await redis.execute("GET", f"author:id:{author_id}")
            if author_str:
                author = json.loads(author_str)
                if author and author not in authors_objects:
                    authors_objects.append(author)
        logger.debug(f"topic#{topic_id} cache updated with {len(authors)} authors")
    return authors_objects
