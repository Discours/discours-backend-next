import asyncio

from sqlalchemy import select, event
import json

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import ShoutAuthor, Shout
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.encoders import CustomJSONEncoder
from services.rediscache import redis
from services.logger import root_logger as logger


DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


async def set_author_cache(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute("SET", f'user:{author.get("user")}:author', payload)
    await redis.execute("SET", f'id:{author.get("id")}:author', payload)


async def update_author_followers_cache(author_id: int, followers):
    payload = json.dumps(
        [f.dict() if isinstance(f, Author) else f for f in followers],
        cls=CustomJSONEncoder,
    )
    await redis.execute("SET", f"author:{author_id}:followers", payload)


async def set_follows_topics_cache(follows, author_id: int):
    try:
        payload = json.dumps(
            [a.dict() if isinstance(a, Author) else a for a in follows],
            cls=CustomJSONEncoder,
        )
        await redis.execute("SET", f"author:{author_id}:follows-topics", payload)
    except Exception as exc:
        logger.error(exc)
        import traceback

        exc = traceback.format_exc()
        logger.error(exc)


async def set_follows_authors_cache(follows, author_id: int):
    try:
        payload = json.dumps(
            [a.dict() if isinstance(a, Author) else a for a in follows],
            cls=CustomJSONEncoder,
        )
        await redis.execute("SET", f"author:{author_id}:follows-authors", payload)
    except Exception as exc:
        import traceback

        logger.error(exc)
        exc = traceback.format_exc()
        logger.error(exc)


async def update_follows_for_author(
    follower: Author, entity_type: str, entity: dict, is_insert: bool
):
    follows = []
    redis_key = f"author:{follower.id}:follows-{entity_type}s"
    follows_str = await redis.execute("GET", redis_key)
    if isinstance(follows_str, str):
        follows = json.loads(follows_str)
    if is_insert:
        follows.append(entity)
    else:
        entity_id = entity.get("id")
        if not entity_id:
            raise Exception("wrong entity")
        # Remove the entity from follows
        follows = [e for e in follows if e["id"] != entity_id]
        logger.debug(f'{entity['slug']} removed from what @{follower.slug} follows')
    if entity_type == "topic":
        await set_follows_topics_cache(follows, follower.id)
    if entity_type == "author":
        await set_follows_authors_cache(follows, follower.id)
    return follows


async def update_followers_for_author(
    follower: Author, author: Author, is_insert: bool
):
    redis_key = f"author:{author.id}:followers"
    followers_str = await redis.execute("GET", redis_key)
    followers = []
    if isinstance(followers_str, str):
        followers = json.loads(followers_str)
    if is_insert:
        followers.append(follower)
    else:
        # Remove the entity from followers
        followers = [e for e in followers if e["id"] != author.id]
    await update_author_followers_cache(author.id, followers)
    return followers


def after_shout_update(_mapper, _connection, shout: Shout):
    # Main query to get authors associated with the shout through ShoutAuthor
    authors_query = (
        select(Author)
        .select_from(ShoutAuthor)  # Select from ShoutAuthor
        .join(Author, Author.id == ShoutAuthor.author)  # Join with Author
        .where(ShoutAuthor.shout == shout.id)  # Filter by shout.id
    )

    for author_with_stat in get_with_stat(authors_query):
        asyncio.create_task(set_author_cache(author_with_stat.dict()))


def after_reaction_update(mapper, connection, reaction: Reaction):
    try:
        author_subquery = select(Author).where(Author.id == reaction.created_by)
        replied_author_subquery = (
            select(Author)
            .join(Reaction, Author.id == Reaction.created_by)
            .where(Reaction.id == reaction.reply_to)
        )

        author_query = (
            select(author_subquery.subquery())
            .select_from(author_subquery.subquery())
            .union(
                select(replied_author_subquery.subquery())
                .select_from(replied_author_subquery.subquery())
            )
        )

        for author_with_stat in get_with_stat(author_query):
            asyncio.create_task(set_author_cache(author_with_stat.dict()))

        shout = connection.execute(
            select(Shout).select_from(Shout).where(Shout.id == reaction.shout)
        ).first()
        if shout:
            after_shout_update(mapper, connection, shout)
    except Exception as exc:
        logger.error(exc)
        import traceback

        traceback.print_exc()


def after_author_update(_mapper, _connection, author: Author):
    q = select(Author).where(Author.id == author.id)
    result = get_with_stat(q)
    if result:
        [author_with_stat] = result
        asyncio.create_task(set_author_cache(author_with_stat.dict()))


def after_topic_follower_insert(_mapper, _connection, target: TopicFollower):
    asyncio.create_task(
        handle_topic_follower_change(target.topic, target.follower, True)
    )


def after_topic_follower_delete(_mapper, _connection, target: TopicFollower):
    asyncio.create_task(
        handle_topic_follower_change(target.topic, target.follower, False)
    )


def after_author_follower_insert(_mapper, _connection, target: AuthorFollower):
    asyncio.create_task(
        handle_author_follower_change(target.author, target.follower, True)
    )


def after_author_follower_delete(_mapper, _connection, target: AuthorFollower):
    asyncio.create_task(
        handle_author_follower_change(target.author, target.follower, False)
    )


async def handle_author_follower_change(
    author_id: int, follower_id: int, is_insert: bool
):
    author_query = select(Author).select_from(Author).filter(Author.id == author_id)
    [author] = get_with_stat(author_query)
    follower_query = select(Author).select_from(Author).filter(Author.id == follower_id)
    [follower] = get_with_stat(follower_query)
    if follower and author:
        _ = asyncio.create_task(set_author_cache(author.dict()))
        follows_authors = await redis.execute(
            "GET", f"author:{follower_id}:follows-authors"
        )
        if follows_authors:
            follows_authors = json.loads(follows_authors)
            if not any(x.get("id") == author.id for x in follows_authors):
                follows_authors.append(author.dict())
        _ = asyncio.create_task(set_follows_authors_cache(follows_authors, follower_id))
        _ = asyncio.create_task(set_author_cache(follower.dict()))
        await update_follows_for_author(
            follower,
            "author",
            {
                "id": author.id,
                "name": author.name,
                "slug": author.slug,
                "pic": author.pic,
                "bio": author.bio,
                "stat": author.stat,
            },
            is_insert,
        )


async def handle_topic_follower_change(
    topic_id: int, follower_id: int, is_insert: bool
):
    topic_query = select(Topic).filter(Topic.id == topic_id)
    [topic] = get_with_stat(topic_query)
    follower_query = select(Author).filter(Author.id == follower_id)
    [follower] = get_with_stat(follower_query)
    if follower and topic:
        _ = asyncio.create_task(set_author_cache(follower.dict()))
        follows_topics = await redis.execute(
            "GET", f"author:{follower_id}:follows-topics"
        )
        if follows_topics:
            follows_topics = json.loads(follows_topics)
            if not any(x.get("id") == topic.id for x in follows_topics):
                follows_topics.append(topic)
        _ = asyncio.create_task(set_follows_topics_cache(follows_topics, follower_id))
        await update_follows_for_author(
            follower,
            "topic",
            {
                "id": topic.id,
                "title": topic.title,
                "slug": topic.slug,
                "body": topic.body,
                "stat": topic.stat,
            },
            is_insert,
        )


def events_register():
    event.listen(Shout, "after_insert", after_shout_update)
    event.listen(Shout, "after_update", after_shout_update)

    event.listen(Reaction, "after_insert", after_reaction_update)
    event.listen(Reaction, "after_update", after_reaction_update)

    event.listen(Author, "after_insert", after_author_update)
    event.listen(Author, "after_update", after_author_update)

    event.listen(AuthorFollower, "after_insert", after_author_follower_insert)
    event.listen(AuthorFollower, "after_delete", after_author_follower_delete)

    event.listen(TopicFollower, "after_insert", after_topic_follower_insert)
    event.listen(TopicFollower, "after_delete", after_topic_follower_delete)

    logger.info('cache events were registered!')
