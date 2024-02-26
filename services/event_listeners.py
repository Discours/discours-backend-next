import asyncio

from sqlalchemy import select, event, or_, exists, and_
import json

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import ShoutAuthor, Shout
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.rediscache import redis
from services.logger import root_logger as logger


DEFAULT_FOLLOWS = {
    'topics': [],
    'authors': [],
    'communities': [{'id': 1, 'name': 'Дискурс', 'slug': 'discours', 'pic': ''}],
}


async def update_author_cache(author: dict, ttl=25 * 60 * 60):
    payload = json.dumps(author)
    await redis.execute('SETEX', f'user:{author.get("user")}:author', ttl, payload)
    await redis.execute('SETEX', f'id:{author.get("id")}:author', ttl, payload)


async def update_follows_topics_cache(follows, author_id: int, ttl=25 * 60 * 60):
    try:
        payload = json.dumps(follows)
        await redis.execute('SETEX', f'author:{author_id}:follows-topics', ttl, payload)
    except Exception:
        import traceback

        exc = traceback.format_exc()
        logger.error(exc)


async def update_follows_authors_cache(follows, author_id: int, ttl=25 * 60 * 60):
    try:
        payload = json.dumps(follows)
        await redis.execute('SETEX', f'author:{author_id}:follows-authors', ttl, payload)
    except Exception:
        import traceback

        exc = traceback.format_exc()
        logger.error(exc)


@event.listens_for(Shout, 'after_insert')
@event.listens_for(Shout, 'after_update')
def after_shouts_update(mapper, connection, shout: Shout):
    # Создаем подзапрос для проверки наличия авторов в списке shout.authors
    subquery = select(1).where(
        or_(
            Author.id == shout.created_by,
            and_(
                Shout.id == shout.id,
                ShoutAuthor.shout == Shout.id,
                ShoutAuthor.author == Author.id,
                ),
            )
    )

    # Основной запрос с использованием объединения и подзапроса exists
    authors_query = (
        select(Author)
        .select_from(Author)
        .join(ShoutAuthor, Author.id == ShoutAuthor.author)
        .where(ShoutAuthor.shout == shout.id)
        .union(select(Author).where(exists(subquery)))
    )
    authors = get_with_stat(authors_query)
    for author in authors:
        asyncio.create_task(update_author_cache(author.dict()))


@event.listens_for(Reaction, 'after_insert')
def after_reaction_insert(mapper, connection, reaction: Reaction):
    author_subquery = select(Author).where(Author.id == reaction.created_by)
    replied_author_subquery = (
        select(Author)
        .select_from(Author)
        .join(Reaction, Author.id == Reaction.created_by)
        .where(Reaction.id == reaction.reply_to)
    )

    author_query = author_subquery.union(replied_author_subquery)
    authors = get_with_stat(author_query)

    for author in authors:
        asyncio.create_task(update_author_cache(author.dict()))

    shout = connection.execute(select(Shout).select_from(Shout).where(Shout.id == reaction.shout)).first()
    if shout:
        after_shouts_update(mapper, connection, shout)


@event.listens_for(Author, 'after_insert')
@event.listens_for(Author, 'after_update')
def after_author_update(mapper, connection, author: Author):
    asyncio.create_task(update_author_cache(author.dict()))


@event.listens_for(TopicFollower, 'after_insert')
def after_topic_follower_insert(mapper, connection, target: TopicFollower):
    asyncio.create_task(
        handle_topic_follower_change(connection, target.topic, target.follower, True)
    )


@event.listens_for(TopicFollower, 'after_delete')
def after_topic_follower_delete(mapper, connection, target: TopicFollower):
    asyncio.create_task(
        handle_topic_follower_change(connection, target.topic, target.follower, False)
    )


@event.listens_for(AuthorFollower, 'after_insert')
def after_author_follower_insert(mapper, connection, target: AuthorFollower):
    asyncio.create_task(
        handle_author_follower_change(connection, target.author, target.follower, True)
    )


@event.listens_for(AuthorFollower, 'after_delete')
def after_author_follower_delete(mapper, connection, target: AuthorFollower):
    asyncio.create_task(
        handle_author_follower_change(connection, target.author, target.follower, False)
    )


async def update_follows_for_author(
    connection, follower, entity_type, entity: dict, is_insert
):
    ttl = 25 * 60 * 60
    redis_key = f'id:{follower.id}:follows-{entity_type}s'
    follows_str = await redis.get(redis_key)
    follows = json.loads(follows_str) if follows_str else []
    if is_insert:
        follows[f'{entity_type}s'].append(entity)
    else:
        # Remove the entity from follows
        follows[f'{entity_type}s'] = [
            e for e in follows[f'{entity_type}s'] if e['id'] != entity['id']
        ]
    await redis.execute('SETEX', redis_key, ttl, json.dumps(follows))


async def handle_author_follower_change(
    connection, author_id: int, follower_id: int, is_insert: bool
):
    author_query = select(Author).filter(Author.id == author_id)
    [author] = get_with_stat(author_query)
    follower_query = select(Author).filter(Author.id == follower_id)
    follower = get_with_stat(follower_query)
    if follower and author:
        _ = asyncio.create_task(update_author_cache(author.dict()))
        follows_authors = await redis.execute('GET', f'author:{follower_id}:follows-authors')
        if follows_authors:
            follows_authors = json.loads(follows_authors)
            if not any(x.get('id') == author.id for x in follows_authors):
                follows_authors.append(author.dict())
        _ = asyncio.create_task(update_follows_authors_cache(follows_authors, follower_id))
        _ = asyncio.create_task(update_author_cache(follower.dict()))
        await update_follows_for_author(
            connection,
            follower,
            'author',
            {
                'id': author.id,
                'name': author.name,
                'slug': author.slug,
                'pic': author.pic,
                'bio': author.bio,
                'stat': author.stat,
            },
            is_insert,
        )


async def handle_topic_follower_change(
    connection, topic_id: int, follower_id: int, is_insert: bool
):
    q = select(Topic).filter(Topic.id == topic_id)
    topics = get_with_stat(q)
    topic = topics[0]
    follower_query = select(Author).filter(Author.id == follower_id)
    follower = get_with_stat(follower_query)
    if follower and topic:
        _ = asyncio.create_task(update_author_cache(follower.dict()))
        follows_topics = await redis.execute('GET', f'author:{follower_id}:follows-topics')
        if follows_topics:
            follows_topics = json.loads(follows_topics)
            if not any(x.get('id') == topic.id for x in follows_topics):
                follows_topics.append(topic)
        _ = asyncio.create_task(update_follows_topics_cache(follows_topics, follower_id))
        await update_follows_for_author(
            connection,
            follower,
            'topic',
            {
                'id': topic.id,
                'title': topic.title,
                'slug': topic.slug,
                'body': topic.body,
                'stat': topic.stat,
            },
            is_insert,
        )
