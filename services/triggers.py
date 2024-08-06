import asyncio
from sqlalchemy import event, select
from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.cache import cache_author, cache_follows, cache_topic
from services.logger import root_logger as logger

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}

# Limit the number of concurrent tasks
semaphore = asyncio.Semaphore(10)


async def run_background_task(coro):
    """Runs an asynchronous task in the background with concurrency control."""
    async with semaphore:
        try:
            await coro
        except Exception as e:
            logger.error(f"Error in background task: {e}")


async def batch_cache_updates(authors, topics, followers):
    tasks = (
        [cache_author(author) for author in authors]
        + [
            cache_follows(follower["id"], follower["type"], follower["item_id"], follower["is_insert"])
            for follower in followers
        ]
        + [cache_topic(topic) for topic in topics]
    )
    await asyncio.gather(*tasks)


async def handle_author_follower_change(author_id: int, follower_id: int, is_insert: bool):
    queries = [select(Author).filter(Author.id == author_id), select(Author).filter(Author.id == follower_id)]
    author_result, follower_result = await asyncio.gather(*(get_with_stat(query) for query in queries))

    if author_result and follower_result:
        authors = [author_result[0].dict()]
        followers = [
            {"id": follower_result[0].id, "type": "author", "item_id": author_result[0].id, "is_insert": is_insert}
        ]
        await batch_cache_updates(authors, [], followers)


async def handle_topic_follower_change(topic_id: int, follower_id: int, is_insert: bool):
    queries = [select(Topic).filter(Topic.id == topic_id), select(Author).filter(Author.id == follower_id)]
    topic_result, follower_result = await asyncio.gather(*(get_with_stat(query) for query in queries))

    if topic_result and follower_result:
        topics = [topic_result[0].dict()]
        followers = [
            {"id": follower_result[0].id, "type": "topic", "item_id": topic_result[0].id, "is_insert": is_insert}
        ]
        await batch_cache_updates([], topics, followers)


async def after_shout_update(_mapper, _connection, shout: Shout):
    authors_query = (
        select(Author).join(ShoutAuthor, ShoutAuthor.author == Author.id).filter(ShoutAuthor.shout == shout.id)
    )
    authors_updated = await get_with_stat(authors_query)
    await batch_cache_updates([author.dict() for author in authors_updated], [], [])


async def after_reaction_update(mapper, connection, reaction: Reaction):
    queries = [
        select(Author).where(Author.id == reaction.created_by),
        select(Author).join(Reaction, Author.id == Reaction.created_by).where(Reaction.id == reaction.reply_to),
    ]
    results = await asyncio.gather(*(get_with_stat(query) for query in queries))
    authors = [result[0].dict() for result in results if result]

    shout_query = select(Shout).where(Shout.id == reaction.shout)
    shout_result = await connection.execute(shout_query)
    shout = shout_result.scalar_one_or_none()

    tasks = [cache_author(author) for author in authors]
    if shout:
        tasks.append(after_shout_update(mapper, connection, shout))
    await asyncio.gather(*tasks)


async def after_author_update(_mapper, _connection, author: Author):
    author_query = select(Author).where(Author.id == author.id)
    result = await get_with_stat(author_query)
    if result:
        await cache_author(result[0].dict())


async def after_author_follower_insert(_mapper, _connection, target: AuthorFollower):
    logger.info(f"Author follower inserted: {target}")
    await handle_author_follower_change(target.author, target.follower, True)


async def after_author_follower_delete(_mapper, _connection, target: AuthorFollower):
    logger.info(f"Author follower deleted: {target}")
    await handle_author_follower_change(target.author, target.follower, False)


async def after_topic_follower_insert(_mapper, _connection, target: TopicFollower):
    logger.info(f"Topic follower inserted: {target}")
    await handle_topic_follower_change(target.topic, target.follower, True)


async def after_topic_follower_delete(_mapper, _connection, target: TopicFollower):
    logger.info(f"Topic follower deleted: {target}")
    await handle_topic_follower_change(target.topic, target.follower, False)


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
    logger.info("Cache events were registered!")
