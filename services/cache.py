import asyncio
import json

from sqlalchemy import event, select

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.rediscache import redis

DEFAULT_FOLLOWS = {
    'topics': [],
    'authors': [],
    'communities': [{'id': 1, 'name': 'Дискурс', 'slug': 'discours', 'pic': ''}],
}


async def set_author_cache(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute('SET', f'user:{author.get("user")}', payload)
    await redis.execute('SET', f'author:{author.get("id")}', payload)

    # update stat all field for followers' caches in <authors> list
    followers_str = await redis.execute('GET', f'author:{author.get("id")}:followers')
    followers = []
    if followers_str:
        followers = json.loads(followers_str)
    if isinstance(followers, list):
        for follower in followers:
            follower_follows_authors = []
            follower_follows_authors_str = await redis.execute('GET', f'author:{author.get("id")}:follows-authors')
            if follower_follows_authors_str:
                follower_follows_authors = json.loads(follower_follows_authors_str)
                c = 0
                for old_author in follower_follows_authors:
                    if int(old_author.get('id')) == int(author.get('id', 0)):
                        follower_follows_authors[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            else:
                # author not found in the list, so add the new author with the updated stat field
                follower_follows_authors.append(author)

    # update stat field for all authors' caches in <followers> list
    follows_str = await redis.execute('GET', f'author:{author.get("id")}:follows-authors')
    follows_authors = []
    if follows_str:
        follows_authors = json.loads(follows_str)
    if isinstance(follows_authors, list):
        for followed_author in follows_authors:
            followed_author_followers = []
            followed_author_followers_str = await redis.execute('GET', f'author:{author.get("id")}:followers')
            if followed_author_followers_str:
                followed_author_followers = json.loads(followed_author_followers_str)
                c = 0
                for old_follower in followed_author_followers:
                    if int(old_follower.get('id')) == int(author.get('id', 0)):
                        followed_author_followers[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            else:
                # author not found in the list, so add the new author with the updated stat field
                followed_author_followers.append(author)

async def update_author_followers_cache(author_id: int, followers):
    updated_followers = [f.dict() if isinstance(f, Author) else f for f in followers]
    payload = json.dumps(
        updated_followers,
        cls=CustomJSONEncoder,
    )
    await redis.execute('SET', f'author:{author_id}:followers', payload)
    author_str = await redis.execute('GET', f'author:{author_id}')
    if author_str:
        author = json.loads(author_str)
        author['stat']['followers'] = len(updated_followers)
        await set_author_cache(author)


async def set_topic_cache(topic: dict):
    payload = json.dumps(topic, cls=CustomJSONEncoder)
    await redis.execute('SET', f'topic:{topic.get("id")}', payload)


async def set_follows_topics_cache(follows, author_id: int):
    try:
        payload = json.dumps(
            [a.dict() if isinstance(a, Author) else a for a in follows],
            cls=CustomJSONEncoder,
        )
        await redis.execute('SET', f'author:{author_id}:follows-topics', payload)
    except Exception as exc:
        logger.error(exc)
        import traceback

        exc = traceback.format_exc()
        logger.error(exc)


async def set_follows_authors_cache(follows, author_id: int):
    updated_follows = [a.dict() if isinstance(a, Author) else a for a in follows]
    try:
        payload = json.dumps(
            updated_follows,
            cls=CustomJSONEncoder,
        )
        await redis.execute('SET', f'author:{author_id}:follows-authors', payload)
        # update author everywhere
        author_str = await redis.execute('GET', f'author:{author_id}')
        if author_str:
            author = json.loads(author_str)
            author['stat']['authors'] = len(updated_follows)
            await set_author_cache(author)
    except Exception as exc:
        import traceback

        logger.error(exc)
        exc = traceback.format_exc()
        logger.error(exc)


async def update_follows_for_author(
    follower: Author, entity_type: str, entity: dict, is_insert: bool
):
    follows = []
    redis_key = f'author:{follower.id}:follows-{entity_type}s'
    follows_str = await redis.execute('GET', redis_key)
    if isinstance(follows_str, str):
        follows = json.loads(follows_str)
    if is_insert:
        follows.append(entity)
    else:
        entity_id = entity.get('id')
        if not entity_id:
            raise Exception('wrong entity')
        # Remove the entity from follows
        follows = [e for e in follows if e['id'] != entity_id]
        logger.debug(f'{entity['slug']} removed from what @{follower.slug} follows')
    if entity_type == 'topic':
        await set_follows_topics_cache(follows, follower.id)
    if entity_type == 'author':
        await set_follows_authors_cache(follows, follower.id)
    return follows


async def update_followers_for_author(
    follower: Author, author: Author, is_insert: bool
):
    redis_key = f'author:{author.id}:followers'
    followers_str = await redis.execute('GET', redis_key)
    followers = []
    if isinstance(followers_str, str):
        followers = json.loads(followers_str)
    if is_insert:
        followers.append(follower)
    else:
        # Remove the entity from followers
        followers = [e for e in followers if e['id'] != author.id]
    await update_author_followers_cache(author.id, followers)
    return followers


def after_shout_update(_mapper, _connection, shout: Shout):
    logger.info('after shout update')
    # Main query to get authors associated with the shout through ShoutAuthor
    authors_query = (
        select(Author)
        .select_from(ShoutAuthor)  # Select from ShoutAuthor
        .join(Author, Author.id == ShoutAuthor.author)  # Join with Author
        .filter(ShoutAuthor.shout == shout.id)  # Filter by shout.id
    )

    for author_with_stat in get_with_stat(authors_query):
        asyncio.create_task(set_author_cache(author_with_stat.dict()))


def after_reaction_update(mapper, connection, reaction: Reaction):
    logger.info('after reaction update')
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
    logger.info('after author update')
    q = select(Author).where(Author.id == author.id)
    result = get_with_stat(q)
    if result:
        [author_with_stat] = result
        asyncio.create_task(set_author_cache(author_with_stat.dict()))


def after_topic_follower_insert(_mapper, _connection, target: TopicFollower):
    logger.info(target)
    asyncio.create_task(
        handle_topic_follower_change(target.topic, target.follower, True)
    )


def after_topic_follower_delete(_mapper, _connection, target: TopicFollower):
    logger.info(target)
    asyncio.create_task(
        handle_topic_follower_change(target.topic, target.follower, False)
    )


def after_author_follower_insert(_mapper, _connection, target: AuthorFollower):
    logger.info(target)
    asyncio.create_task(
        handle_author_follower_change(target.author, target.follower, True)
    )


def after_author_follower_delete(_mapper, _connection, target: AuthorFollower):
    logger.info(target)
    asyncio.create_task(
        handle_author_follower_change(target.author, target.follower, False)
    )


async def handle_author_follower_change(
    author_id: int, follower_id: int, is_insert: bool
):
    logger.info(author_id)
    author_query = select(Author).select_from(Author).filter(Author.id == author_id)
    [author] = get_with_stat(author_query)
    follower_query = select(Author).select_from(Author).filter(Author.id == follower_id)
    [follower] = get_with_stat(follower_query)
    if follower and author:
        _ = asyncio.create_task(set_author_cache(author.dict()))
        follows_authors = await redis.execute(
            'GET', f'author:{follower_id}:follows-authors'
        )
        if isinstance(follows_authors, str):
            follows_authors = json.loads(follows_authors)
            if not any(x.get('id') == author.id for x in follows_authors):
                follows_authors.append(author.dict())
        _ = asyncio.create_task(set_follows_authors_cache(follows_authors, follower_id))
        _ = asyncio.create_task(set_author_cache(follower.dict()))
        await update_follows_for_author(
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
    topic_id: int, follower_id: int, is_insert: bool
):
    logger.info(topic_id)
    topic_query = select(Topic).filter(Topic.id == topic_id)
    [topic] = get_with_stat(topic_query)
    follower_query = select(Author).filter(Author.id == follower_id)
    [follower] = get_with_stat(follower_query)
    if follower and topic:
        _ = asyncio.create_task(set_author_cache(follower.dict()))
        follows_topics = await redis.execute(
            'GET', f'author:{follower_id}:follows-topics'
        )
        if isinstance(follows_topics, str):
            follows_topics = json.loads(follows_topics)
            if not any(x.get('id') == topic.id for x in follows_topics):
                follows_topics.append(topic)
        _ = asyncio.create_task(set_follows_topics_cache(follows_topics, follower_id))
        await update_follows_for_author(
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


def events_register():
    event.listen(Shout, 'after_insert', after_shout_update)
    event.listen(Shout, 'after_update', after_shout_update)

    event.listen(Reaction, 'after_insert', after_reaction_update)
    event.listen(Reaction, 'after_update', after_reaction_update)

    event.listen(Author, 'after_insert', after_author_update)
    event.listen(Author, 'after_update', after_author_update)

    event.listen(AuthorFollower, 'after_insert', after_author_follower_insert)
    event.listen(AuthorFollower, 'after_delete', after_author_follower_delete)

    event.listen(TopicFollower, 'after_insert', after_topic_follower_insert)
    event.listen(TopicFollower, 'after_delete', after_topic_follower_delete)

    logger.info('cache events were registered!')
