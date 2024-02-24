import asyncio

from sqlalchemy import select, event, or_, exists, and_
import json

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import ShoutAuthor, Shout
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_authors_with_stat, get_topics_with_stat
from services.rediscache import redis


DEFAULT_FOLLOWS = {
    'topics': [],
    'authors': [],
    'communities': [{'slug': 'discours', 'name': 'Дискурс', 'id': 1, 'pic': ''}],
}


async def update_author_cache(author: Author, ttl=25 * 60 * 60):
    payload = json.dumps(author.dict())
    await redis.execute('SETEX', f'user:{author.user}:author', ttl, payload)
    await redis.execute('SETEX', f'id:{author.user}:author', ttl, payload)


@event.listens_for(Shout, 'after_insert')
@event.listens_for(Shout, 'after_update')
def after_shouts_update(mapper, connection, shout: Shout):
    # Создаем подзапрос для проверки наличия авторов в списке shout.authors
    subquery = (
        select(1)
        .where(or_(
            Author.id == int(shout.created_by),
            and_(
                Shout.id == shout.id,
                ShoutAuthor.shout == Shout.id,
                ShoutAuthor.author == Author.id
            )
        ))
    )

    # Основной запрос с использованием объединения и подзапроса exists
    authors_query = (
        select(Author)
        .join(ShoutAuthor, Author.id == int(ShoutAuthor.author))
        .where(ShoutAuthor.shout == int(shout.id))
        .union(
            select(Author)
            .where(exists(subquery))
        )
    )
    authors = get_authors_with_stat(authors_query, ratings=True)
    for author in authors:
        asyncio.create_task(update_author_cache(author))


@event.listens_for(Reaction, 'after_insert')
def after_reaction_insert(mapper, connection, reaction: Reaction):
    author_subquery = (
        select(Author)
        .where(Author.id == int(reaction.created_by))
    )
    replied_author_subquery = (
        select(Author)
        .join(Reaction, Author.id == int(Reaction.created_by))
        .where(Reaction.id == int(reaction.reply_to))
    )

    author_query = author_subquery.union(replied_author_subquery)
    authors = get_authors_with_stat(author_query, ratings=True)

    for author in authors:
        asyncio.create_task(update_author_cache(author))

    shout = connection.execute(select(Shout).where(Shout.id == int(reaction.shout))).first()
    if shout:
        after_shouts_update(mapper, connection, shout)



@event.listens_for(Author, 'after_insert')
@event.listens_for(Author, 'after_update')
def after_author_update(mapper, connection, author: Author):
    asyncio.create_task(update_author_cache(author))


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


async def update_follows_for_user(connection, user_id, entity_type, entity: dict, is_insert):
    redis_key = f'user:{user_id}:follows'
    follows_str = await redis.get(redis_key)
    if follows_str:
        follows = json.loads(follows_str)
    else:
        follows = DEFAULT_FOLLOWS
    if is_insert:
        follows[f'{entity_type}s'].append(entity)
    else:
        # Remove the entity from follows
        follows[f'{entity_type}s'] = [e for e in follows[f'{entity_type}s'] if e['id'] != entity['id']]
    await redis.execute('SET', redis_key, json.dumps(follows))


async def handle_author_follower_change(connection, author_id: int, follower_id: int, is_insert: bool):
    author_query = select(Author).filter(Author.id == author_id)
    [author, ] = get_authors_with_stat(author_query, ratings=True)
    follower_query = select(Author).filter(Author.id == follower_id)
    follower = get_authors_with_stat(follower_query, ratings=True)
    if follower and author:
        _ = asyncio.create_task(update_author_cache(author))
        _ = asyncio.create_task(update_author_cache(follower))
        await update_follows_for_user(
            connection,
            follower.user,
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


async def handle_topic_follower_change(connection, topic_id: int, follower_id: int, is_insert: bool):
    q = select(Topic).filter(Topic.id == topic_id)
    topics = get_topics_with_stat(q)
    topic = topics[0]
    follower_query = select(Author).filter(Author.id == follower_id)
    follower = get_authors_with_stat(follower_query, ratings=True)
    if follower and topic:
        _ = asyncio.create_task(update_author_cache(follower))
        await update_follows_for_user(
            connection,
            follower.user,
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
