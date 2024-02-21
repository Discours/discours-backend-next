import asyncio
from aiocron import crontab
from sqlalchemy import select, event

from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from resolvers.author import add_author_stat_columns, get_author_follows
from resolvers.topic import add_topic_stat_columns
from services.db import local_session
from services.rediscache import redis
from services.viewed import ViewedStorage


async def update_cache():
    with local_session() as session:
        for author in session.query(Author).all():
            redis_key = f"user:{author.user}:author"
            await redis.hset(redis_key, **vars(author))
            follows = await get_author_follows(None, None, user=author.user)
            if isinstance(follows, dict):
                redis_key = f"user:{author.user}:follows"
                await redis.hset(redis_key, **follows)


@crontab("*/10 * * * *", func=update_cache)
async def scheduled_cache_update():
    pass


@event.listens_for(Author, "after_insert")
@event.listens_for(Author, "after_update")
def after_author_update(mapper, connection, target):
    redis_key = f"user:{target.user}:author"
    asyncio.create_task(redis.hset(redis_key, **vars(target)))


@event.listens_for(TopicFollower, "after_insert")
def after_topic_follower_insert(mapper, connection, target):
    asyncio.create_task(
        handle_topic_follower_change(connection, target.topic, target.follower, True)
    )


@event.listens_for(TopicFollower, "after_delete")
def after_topic_follower_delete(mapper, connection, target):
    asyncio.create_task(
        handle_topic_follower_change(connection, target.topic, target.follower, False)
    )


@event.listens_for(AuthorFollower, "after_insert")
def after_author_follower_insert(mapper, connection, target):
    asyncio.create_task(
        handle_author_follower_change(connection, target.author, target.follower, True)
    )


@event.listens_for(AuthorFollower, "after_delete")
def after_author_follower_delete(mapper, connection, target):
    asyncio.create_task(
        handle_author_follower_change(connection, target.author, target.follower, False)
    )


async def update_follows_for_user(connection, user_id, entity_type, entity, is_insert):
    redis_key = f"user:{user_id}:follows"
    follows = await redis.hget(redis_key)
    if not follows:
        follows = {
            "topics": [],
            "authors": [],
            "communities": [
                {"slug": "discours", "name": "Дискурс", "id": 1, "desc": ""}
            ],
        }
    if is_insert:
        follows[f"{entity_type}s"].append(entity)
    else:
        # Remove the entity from follows
        follows[f"{entity_type}s"] = [
            e for e in follows[f"{entity_type}s"] if e["id"] != entity.id
        ]

    await redis.hset(redis_key, **vars(follows))


async def handle_author_follower_change(connection, author_id, follower_id, is_insert):
    q = select(Author).filter(Author.id == author_id)
    q = add_author_stat_columns(q)
    async with connection.begin() as conn:
        [author, shouts_stat, followers_stat, followings_stat] = await conn.execute(
            q
        ).first()
        author.stat = {
            "shouts": shouts_stat,
            "viewed": await ViewedStorage.get_author(author.slug),
            "followers": followers_stat,
            "followings": followings_stat,
        }
        follower = await conn.execute(
            select(Author).filter(Author.id == follower_id)
        ).first()
        if follower and author:
            await update_follows_for_user(
                connection, follower.user, "author", author, is_insert
            )


async def handle_topic_follower_change(connection, topic_id, follower_id, is_insert):
    q = select(Topic).filter(Topic.id == topic_id)
    q = add_topic_stat_columns(q)
    async with connection.begin() as conn:
        [topic, shouts_stat, authors_stat, followers_stat] = await conn.execute(
            q
        ).first()
        topic.stat = {
            "shouts": shouts_stat,
            "authors": authors_stat,
            "followers": followers_stat,
            "viewed": await ViewedStorage.get_topic(topic.slug),
        }
        follower = connection.execute(
            select(Author).filter(Author.id == follower_id)
        ).first()
        if follower and topic:
            await update_follows_for_user(
                connection, follower.user, "topic", topic, is_insert
            )
