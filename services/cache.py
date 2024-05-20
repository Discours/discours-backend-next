import json

from sqlalchemy import and_, join, select

from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_topic_authors_query, get_with_stat
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
    await redis.execute(
        "SET",
        f"topic:id:{topic.get('id')}",
        json.dumps(topic.dict(), cls=CustomJSONEncoder),
    )
    await redis.execute("SET", f"topic:slug:{topic.get('slug')}", topic.id)


async def cache_author(author: dict):
    author_id = author.get("id")
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute("SET", f'user:id:{author.get("user")}', author_id)
    await redis.execute("SET", f"author:id:{author_id}", payload)

    # update stat all field for followers' caches in <authors> list
    followers_str = await redis.execute("GET", f"author:{author_id}:followers")
    followers = []
    if isinstance(followers_str, str):
        followers = json.loads(followers_str)
    if isinstance(followers, list):
        for follower in followers:
            follower_follows_authors = []
            follower_follows_authors_str = await redis.execute(
                "GET", f"author:{author_id}:follows-authors"
            )
            if isinstance(follower_follows_authors_str, str):
                follower_follows_authors = json.loads(follower_follows_authors_str)
                c = 0
                for old_author in follower_follows_authors:
                    if int(old_author.get("id")) == int(author.get("id", 0)):
                        follower_follows_authors[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            else:
                # author not found in the list, so add the new author with the updated stat field
                follower_follows_authors.append(author)

    # update stat field for all authors' caches in <followers> list
    follows_str = await redis.execute("GET", f"author:{author_id}:follows-authors")
    follows_authors = []
    if isinstance(follows_str, str):
        follows_authors = json.loads(follows_str)
    if isinstance(follows_authors, list):
        for followed_author in follows_authors:
            followed_author_followers = []
            followed_author_followers_str = await redis.execute(
                "GET", f"author:{author_id}:followers"
            )
            if isinstance(followed_author_followers_str, str):
                followed_author_followers = json.loads(followed_author_followers_str)
                c = 0
                for old_follower in followed_author_followers:
                    old_follower_id = int(old_follower.get("id"))
                    if old_follower_id == author_id:
                        followed_author_followers[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            # author not found in the list, so add the new author with the updated stat field
            followed_author_followers.append(author)
            await redis.execute(
                "SET",
                f"author:{author_id}:followers",
                json.dumps(followed_author_followers, cls=CustomJSONEncoder),
            )


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
    return follows


async def get_cached_author(author_id: int):
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


async def get_cached_author_by_user_id(user_id: str):
    author_id = await redis.execute("GET", f"user:id:{user_id}")
    if author_id:
        return await get_cached_author(int(author_id))


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
    return topics


async def get_cached_author_follows_authors(author_id: int):
    authors = []
    rkey = f"author:follows-authors:{author_id}"
    cached = await redis.execute("GET", rkey)
    if not cached:
        with local_session() as session:
            authors = (
                session.query(Author.id)
                .select_from(
                    join(Author, AuthorFollower, Author.id == AuthorFollower.author)
                )
                .where(AuthorFollower.follower == author_id)
                .all()
            )
            await redis.execute("SET", rkey, json.dumps(authors))
    elif isinstance(cached, str):
        authors = json.loads(cached)
    return authors


async def get_cached_author_followers(author_id: int):
    followers = []
    rkey = f"author:followers:{author_id}"
    cached = await redis.execute("GET", rkey)
    if isinstance(cached, str):
        followers = json.loads(cached)
        if isinstance(followers, list):
            return followers

    followers = (
        local_session()
        .query(Author.id)
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
    if followers:
        await redis.execute("SET", rkey, json.dumps(followers))
    logger.debug(f"author#{author_id} cache updated with {len(followers)} followers")
    return followers


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
        .query(Author.id)
        .join(
            TopicFollower,
            and_(TopicFollower.topic == topic_id, TopicFollower.follower == Author.id),
        )
        .all()
    )
    if followers:
        await redis.execute("SET", rkey, json.dumps(followers))
    logger.debug(f"topic#{topic_id} cache updated with {len(followers)} followers")
    return followers


async def get_cached_topic_authors(topic_id: int):
    authors = []
    rkey = f"topic:authors:{topic_id}"
    cached = await redis.execute("GET", rkey)
    if isinstance(cached, str):
        authors = json.loads(cached)
        if isinstance(authors, list):
            return authors

    authors = local_session().execute(get_topic_authors_query(topic_id))
    # should be id list
    if authors:
        await redis.execute("SET", rkey, json.dumps(authors))
    logger.debug(f"topic#{topic_id} cache updated with {len(authors)} authors")
    return authors
