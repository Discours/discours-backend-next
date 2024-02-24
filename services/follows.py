import asyncio

from sqlalchemy import select, event
import json

from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_authors_with_stat, get_topics_with_stat
from services.rediscache import redis


DEFAULT_FOLLOWS = {
    'topics': [],
    'authors': [],
    'communities': [{'slug': 'discours', 'name': 'Дискурс', 'id': 1, 'pic': ''}],
}


async def update_author(author: Author, ttl=25 * 60 * 60):
    payload = json.dumps(author.dict())
    redis_key = f'user:{author.user}:author'
    await redis.execute('SETEX', redis_key, ttl, payload)
    redis_key = f'author:{author.id}:author'
    await redis.execute('SETEX', redis_key, ttl, payload)


@event.listens_for(Author, 'after_insert')
@event.listens_for(Author, 'after_update')
def after_author_update(mapper, connection, author: Author):
    asyncio.create_task(update_author(author))


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


async def update_follows_for_user(
    connection, user_id, entity_type, entity: dict, is_insert
):
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
        follows[f'{entity_type}s'] = [
            e for e in follows[f'{entity_type}s'] if e['id'] != entity['id']
        ]
    await redis.execute('SET', redis_key, json.dumps(follows))


async def handle_author_follower_change(connection, author_id, follower_id, is_insert):
    q = select(Author).filter(Author.id == author_id)
    authors = get_authors_with_stat(q)
    author = authors[0]
    async with connection.begin() as conn:
        follower = await conn.execute(
            select(Author).filter(Author.id == follower_id)
        ).first()
        if follower and author:
            await update_follows_for_user(
                conn,
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


async def handle_topic_follower_change(connection, topic_id, follower_id, is_insert):
    q = select(Topic).filter(Topic.id == topic_id)
    topics = get_topics_with_stat(q)
    topic = topics[0]

    async with connection.begin() as conn:
        follower = conn.execute(select(Author).filter(Author.id == follower_id)).first()
        if follower and topic:
            await update_follows_for_user(
                conn,
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
