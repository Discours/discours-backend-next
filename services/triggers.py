import asyncio
import json

from sqlalchemy import event, select

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.cache import cache_author, cache_follower, cache_follows
from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.rediscache import redis

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


async def handle_author_follower_change(
    author_id: int, follower_id: int, is_insert: bool
):
    logger.info(author_id)
    author_query = select(Author).select_from(Author).filter(Author.id == author_id)
    [author] = get_with_stat(author_query)
    follower_query = select(Author).select_from(Author).filter(Author.id == follower_id)
    [follower] = get_with_stat(follower_query)
    if follower and author:
        await cache_author(author.dict())
        await cache_author(follower.dict())
        await cache_follows(follower.dict(), "author", author.dict(), is_insert)
        await cache_follower(follower.dict(), author.dict(), is_insert)


async def handle_topic_follower_change(
    topic_id: int, follower_id: int, is_insert: bool
):
    logger.info(topic_id)
    topic_query = select(Topic).filter(Topic.id == topic_id)
    [topic] = get_with_stat(topic_query)
    follower_query = select(Author).filter(Author.id == follower_id)
    [follower] = get_with_stat(follower_query)
    if follower and topic:
        await cache_author(follower.dict())
        await redis.execute(
            "SET", f"topic:{topic.id}", json.dumps(topic.dict(), cls=CustomJSONEncoder)
        )
        await cache_follows(follower.dict(), "topic", topic.dict(), is_insert)


# handle_author_follow and handle_topic_follow -> cache_author, cache_follows, cache_followers


def after_shout_update(_mapper, _connection, shout: Shout):
    logger.info("after shout update")
    # Main query to get authors associated with the shout through ShoutAuthor
    authors_query = (
        select(Author)
        .select_from(ShoutAuthor)  # Select from ShoutAuthor
        .join(Author, Author.id == ShoutAuthor.author)  # Join with Author
        .filter(ShoutAuthor.shout == shout.id)  # Filter by shout.id
    )

    for author_with_stat in get_with_stat(authors_query):
        asyncio.create_task(cache_author(author_with_stat.dict()))


def after_reaction_update(mapper, connection, reaction: Reaction):
    logger.info("after reaction update")
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
                select(replied_author_subquery.subquery()).select_from(
                    replied_author_subquery.subquery()
                )
            )
        )

        for author_with_stat in get_with_stat(author_query):
            asyncio.create_task(cache_author(author_with_stat.dict()))

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
    logger.info("after author update")
    q = select(Author).where(Author.id == author.id)
    result = get_with_stat(q)
    if result:
        [author_with_stat] = result
        if author_with_stat:
            _task = asyncio.create_task(cache_author(author_with_stat.dict()))


def after_topic_follower_insert(_mapper, _connection, target: TopicFollower):
    logger.info(target)
    asyncio.create_task(
        handle_topic_follower_change(target.topic, target.follower, True)  # type: ignore
    )


def after_topic_follower_delete(_mapper, _connection, target: TopicFollower):
    logger.info(target)
    asyncio.create_task(
        handle_topic_follower_change(target.topic, target.follower, False)  # type: ignore
    )


def after_author_follower_insert(_mapper, _connection, target: AuthorFollower):
    logger.info(target)
    asyncio.create_task(
        handle_author_follower_change(target.author, target.follower, True)  # type: ignore
    )


def after_author_follower_delete(_mapper, _connection, target: AuthorFollower):
    logger.info(target)
    asyncio.create_task(
        handle_author_follower_change(target.author, target.follower, False)  # type: ignore
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

    logger.info("cache events were registered!")
