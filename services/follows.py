import asyncio
from sqlalchemy import select, event
import json

from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from resolvers.author import add_author_stat_columns, get_author_follows
from resolvers.topic import add_topic_stat_columns
from services.logger import root_logger as logger
from services.db import local_session
from services.rediscache import redis
from services.viewed import ViewedStorage


@event.listens_for(Author, 'after_insert')
@event.listens_for(Author, 'after_update')
def after_author_update(mapper, connection, author: Author):
    redis_key = f'user:{author.user}:author'
    asyncio.create_task(
        redis.execute(
            'set',
            redis_key,
            json.dumps(
                {
                    'id': author.id,
                    'name': author.name,
                    'slug': author.slug,
                    'pic': author.pic,
                }
            ),
        )
    )


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
        follows = {
            'topics': [],
            'authors': [],
            'communities': [
                {'slug': 'discours', 'name': 'Дискурс', 'id': 1, 'desc': ''}
            ],
        }
    if is_insert:
        follows[f'{entity_type}s'].append(entity)
    else:
        # Remove the entity from follows
        follows[f'{entity_type}s'] = [
            e for e in follows[f'{entity_type}s'] if e['id'] != entity['id']
        ]
    await redis.execute('set', redis_key, json.dumps(follows))


async def handle_author_follower_change(connection, author_id, follower_id, is_insert):
    q = select(Author).filter(Author.id == author_id)
    q = add_author_stat_columns(q)
    async with connection.begin() as conn:
        [author, shouts_stat, followers_stat, followings_stat] = await conn.execute(
            q
        ).first()
        author.stat = {
            'shouts': shouts_stat,
            'viewed': await ViewedStorage.get_author(author.slug),
            'followers': followers_stat,
            'followings': followings_stat,
        }
        follower = await conn.execute(
            select(Author).filter(Author.id == follower_id)
        ).first()
        if follower and author:
            await update_follows_for_user(
                connection, follower.user, 'author', {
                    "id": author.id,
                    "name": author.name,
                    "slug": author.slug,
                    "pic": author.pic,
                    "bio": author.bio,
                    "stat": author.stat
                }, is_insert
            )


async def handle_topic_follower_change(connection, topic_id, follower_id, is_insert):
    q = select(Topic).filter(Topic.id == topic_id)
    q = add_topic_stat_columns(q)
    async with connection.begin() as conn:
        [topic, shouts_stat, authors_stat, followers_stat] = await conn.execute(
            q
        ).first()
        topic.stat = {
            'shouts': shouts_stat,
            'authors': authors_stat,
            'followers': followers_stat,
            'viewed': await ViewedStorage.get_topic(topic.slug),
        }
        follower = connection.execute(
            select(Author).filter(Author.id == follower_id)
        ).first()
        if follower and topic:
            await update_follows_for_user(
                connection, follower.user, 'topic', {
                    "id": topic.id,
                    "title": topic.title,
                    "slug": topic.slug,
                    "body": topic.body,
                    "stat": topic.stat
                }, is_insert
            )

BATCH_SIZE = 33

class FollowsCached:
    lock = asyncio.Lock()

    @staticmethod
    async def update_cache():
        with local_session() as session:
            q = select(Author)
            q = add_author_stat_columns(q)
            authors = session.execute(q)
            for i in range(0, len(authors), BATCH_SIZE):
                batch_authors = authors[i : i + BATCH_SIZE]
                await asyncio.gather(
                    *[
                        FollowsCached.update_author_cache(author)
                        for author in batch_authors
                    ]
                )

    @staticmethod
    async def update_author_cache(author: Author):
        redis_key = f'user:{author.user}:author'
        if isinstance(author, Author):
            await redis.execute(
                'set',
                redis_key,
                json.dumps(
                    {
                        'id': author.id,
                        'name': author.name,
                        'slug': author.slug,
                        'pic': author.pic,
                        'bio': author.bio,
                        'stat': author.stat
                    }
                ),
            )
        follows = await get_author_follows(None, None, user=author.user)
        if isinstance(follows, dict):
            redis_key = f'user:{author.user}:follows'
            await redis.execute('set', redis_key, json.dumps(follows))

    @staticmethod
    async def worker():
        """Асинхронная задача обновления"""
        self = FollowsCached
        while True:
            try:
                await self.update_cache()
                await asyncio.sleep(10 * 60 * 60)
            except asyncio.CancelledError:
                # Handle cancellation due to SIGTERM
                logger.info('Cancellation requested. Cleaning up...')
                # Perform any necessary cleanup before exiting the loop
                break
            except Exception as exc:
                logger.error(exc)


async def start_cached_follows():
    await FollowsCached.worker()
