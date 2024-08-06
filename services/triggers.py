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


def run_background_task(coro):
    """Запускает асинхронную задачу в фоне и обрабатывает исключения."""
    task = asyncio.create_task(coro)
    task.add_done_callback(handle_task_result)


def handle_task_result(task):
    """Обработка результата завершенной задачи."""
    try:
        task.result()
    except Exception as e:
        logger.error(f"Error in background task: {e}")


async def handle_author_follower_change(author_id: int, follower_id: int, is_insert: bool):
    logger.info(
        f"Handling author follower change: author_id={author_id}, follower_id={follower_id}, is_insert={is_insert}"
    )

    author_query = select(Author).filter(Author.id == author_id)
    author_result = await get_with_stat(author_query)

    follower_query = select(Author).filter(Author.id == follower_id)
    follower_result = await get_with_stat(follower_query)

    if follower_result and author_result:
        author_with_stat = author_result[0]
        follower = follower_result[0]
        if author_with_stat:
            author_dict = author_with_stat.dict()
            run_background_task(cache_author(author_dict))
            run_background_task(cache_follows(follower.id, "author", author_with_stat.id, is_insert))


async def handle_topic_follower_change(topic_id: int, follower_id: int, is_insert: bool):
    logger.info(
        f"Handling topic follower change: topic_id={topic_id}, follower_id={follower_id}, is_insert={is_insert}"
    )

    topic_query = select(Topic).filter(Topic.id == topic_id)
    topic = await get_with_stat(topic_query)

    follower_query = select(Author).filter(Author.id == follower_id)
    follower = await get_with_stat(follower_query)

    if isinstance(follower[0], Author) and isinstance(topic[0], Topic):
        topic = topic[0]
        follower = follower[0]
        run_background_task(cache_topic(topic.dict()))
        run_background_task(cache_author(follower.dict()))
        run_background_task(cache_follows(follower.id, "topic", topic.id, is_insert))


async def after_shout_update(_mapper, _connection, shout: Shout):
    logger.info("after shout update")

    authors_query = (
        select(Author)
        .join(ShoutAuthor, ShoutAuthor.author == Author.id)  # Use join directly with Author
        .filter(ShoutAuthor.shout == shout.id)
    )

    authors_updated = await get_with_stat(authors_query)

    for author_with_stat in authors_updated:
        run_background_task(cache_author(author_with_stat.dict()))


async def after_reaction_update(mapper, connection, reaction: Reaction):
    logger.info("after reaction update")
    try:
        # reaction author
        author_subquery = select(Author).where(Author.id == reaction.created_by)

        result = await get_with_stat(author_subquery)
        if result:
            author_with_stat = result[0]
            if isinstance(author_with_stat, Author):
                author_dict = author_with_stat.dict()
                run_background_task(cache_author(author_dict))

        # reaction repliers
        replied_author_subquery = (
            select(Author).join(Reaction, Author.id == Reaction.created_by).where(Reaction.id == reaction.reply_to)
        )
        authors_with_stat = await get_with_stat(replied_author_subquery)
        for author_with_stat in authors_with_stat:
            run_background_task(cache_author(author_with_stat.dict()))

        shout_query = select(Shout).where(Shout.id == reaction.shout)
        shout_result = await connection.execute(shout_query)
        shout = shout_result.scalar_one_or_none()
        if shout:
            await after_shout_update(mapper, connection, shout)
    except Exception as exc:
        logger.error(exc)
        import traceback

        traceback.print_exc()


async def after_author_update(_mapper, _connection, author: Author):
    logger.info("after author update")
    author_query = select(Author).where(Author.id == author.id)
    result = await get_with_stat(author_query)
    if result:
        author_with_stat = result[0]
        author_dict = author_with_stat.dict()
        run_background_task(cache_author(author_dict))


async def after_topic_follower_insert(_mapper, _connection, target: TopicFollower):
    logger.info(target)
    run_background_task(handle_topic_follower_change(target.topic, target.follower, True))


async def after_topic_follower_delete(_mapper, _connection, target: TopicFollower):
    logger.info(target)
    run_background_task(handle_topic_follower_change(target.topic, target.follower, False))


async def after_author_follower_insert(_mapper, _connection, target: AuthorFollower):
    logger.info(target)
    run_background_task(handle_author_follower_change(target.author, target.follower, True))


async def after_author_follower_delete(_mapper, _connection, target: AuthorFollower):
    logger.info(target)
    run_background_task(handle_author_follower_change(target.author, target.follower, False))


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
