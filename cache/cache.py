import asyncio
import json
from typing import List
from sqlalchemy import select, join, and_
from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from services.db import local_session
from utils.encoders import CustomJSONEncoder
from services.redis import redis
from utils.logger import root_logger as logger

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "shouts": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


# Cache topic data
async def cache_topic(topic: dict):
    payload = json.dumps(topic, cls=CustomJSONEncoder)
    # Cache by id and slug for quick access
    await asyncio.gather(
        redis.execute("SET", f"topic:id:{topic['id']}", payload),
        redis.execute("SET", f"topic:slug:{topic['slug']}", payload),
    )


# Cache author data
async def cache_author(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    # Cache author data by user and id
    await asyncio.gather(
        redis.execute("SET", f"author:user:{author['user'].strip()}", str(author["id"])),
        redis.execute("SET", f"author:id:{author['id']}", payload),
    )


# Cache follows data
async def cache_follows(follower_id: int, entity_type: str, entity_id: int, is_insert=True):
    key = f"author:follows-{entity_type}s:{follower_id}"
    follows_str = await redis.execute("get", key)
    follows = json.loads(follows_str) if follows_str else DEFAULT_FOLLOWS[entity_type]
    if is_insert:
        if entity_id not in follows:
            follows.append(entity_id)
    else:
        follows = [eid for eid in follows if eid != entity_id]
    await redis.execute("set", key, json.dumps(follows, cls=CustomJSONEncoder))
    await update_follower_stat(follower_id, entity_type, len(follows))


# Update follower statistics
async def update_follower_stat(follower_id, entity_type, count):
    follower_key = f"author:id:{follower_id}"
    follower_str = await redis.execute("get", follower_key)
    follower = json.loads(follower_str) if follower_str else None
    if follower:
        follower["stat"] = {f"{entity_type}s": count}
        await cache_author(follower)


# Get author from cache
async def get_cached_author(author_id: int, get_with_stat):
    author_key = f"author:id:{author_id}"
    result = await redis.execute("get", author_key)
    if result:
        return json.loads(result)
    # Load from database if not found in cache
    q = select(Author).where(Author.id == author_id)
    author = get_with_stat(q)
    if author:
        await cache_author(author.dict())
        return author.dict()
    return None


# Function to get cached topic
async def get_cached_topic(topic_id: int):
    """
    Fetch topic data from cache or database by id.

    Args:
        topic_id (int): The identifier for the topic.

    Returns:
        dict: Topic data or None if not found.
    """
    topic_key = f"topic:id:{topic_id}"
    cached_topic = await redis.execute("get", topic_key)
    if cached_topic:
        return json.loads(cached_topic)

    # If not in cache, fetch from the database
    with local_session() as session:
        topic = session.execute(select(Topic).where(Topic.id == topic_id)).scalar_one_or_none()
        if topic:
            topic_dict = topic.dict()
            await redis.execute("set", topic_key, json.dumps(topic_dict, cls=CustomJSONEncoder))
            return topic_dict

    return None


# Get topic by slug from cache
async def get_cached_topic_by_slug(slug: str, get_with_stat):
    topic_key = f"topic:slug:{slug}"
    result = await redis.execute("get", topic_key)
    if result:
        return json.loads(result)
    # Load from database if not found in cache
    topic_query = select(Topic).where(Topic.slug == slug)
    topic = get_with_stat(topic_query)
    if topic:
        topic_dict = topic.dict()
        await cache_topic(topic_dict)
        return topic_dict
    return None


# Get list of authors by ID from cache
async def get_cached_authors_by_ids(author_ids: List[int]) -> List[dict]:
    # Fetch all author data concurrently
    keys = [f"author:id:{author_id}" for author_id in author_ids]
    results = await asyncio.gather(*(redis.execute("get", key) for key in keys))
    authors = [json.loads(result) if result else None for result in results]
    # Load missing authors from database and cache
    missing_indices = [index for index, author in enumerate(authors) if author is None]
    if missing_indices:
        missing_ids = [author_ids[index] for index in missing_indices]
        with local_session() as session:
            query = select(Author).where(Author.id.in_(missing_ids))
            missing_authors = session.execute(query).scalars().all()
            await asyncio.gather(*(cache_author(author.dict()) for author in missing_authors))
            for index, author in zip(missing_indices, missing_authors):
                authors[index] = author.dict()
    return authors


async def get_cached_topic_followers(topic_id: int):
    """
    Получает подписчиков темы по ID, используя кеш Redis.
    Если данные отсутствуют в кеше, извлекает из базы данных и кеширует их.

    :param topic_id: Идентификатор темы, подписчиков которой необходимо получить.
    :return: Список подписчиков темы, каждый элемент представляет собой словарь с ID и именем автора.
    """
    try:
        # Попытка получить данные из кеша
        cached = await redis.get(f"topic:followers:{topic_id}")
        if cached:
            followers_ids = json.loads(cached)
            logger.debug(f"Cached {len(followers_ids)} followers for topic #{topic_id}")
            followers = await get_cached_authors_by_ids(followers_ids)
            return followers

        # Если данные не найдены в кеше, загрузка из базы данных
        async with local_session() as session:
            result = await session.execute(
                session.query(Author.id)
                .join(TopicFollower, TopicFollower.follower == Author.id)
                .filter(TopicFollower.topic == topic_id)
            )
            followers_ids = [f[0] for f in result.scalars().all()]

            # Кеширование результатов
            await redis.set(f"topic:followers:{topic_id}", json.dumps(followers_ids))

            # Получение подробной информации о подписчиках по их ID
            followers = await get_cached_authors_by_ids(followers_ids)
            logger.debug(followers)
            return followers
    except Exception as e:
        logger.error(f"Ошибка при получении подписчиков для темы #{topic_id}: {str(e)}")
        return []


# Get cached author followers
async def get_cached_author_followers(author_id: int):
    # Check cache for data
    cached = await redis.execute("get", f"author:followers:{author_id}")
    if cached:
        followers_ids = json.loads(cached)
        followers = await get_cached_authors_by_ids(followers_ids)
        logger.debug(f"Cached followers for author #{author_id}: {len(followers)}")
        return followers

    # Query database if cache is empty
    with local_session() as session:
        followers_ids = [
            f[0]
            for f in session.query(Author.id)
            .join(AuthorFollower, AuthorFollower.follower == Author.id)
            .filter(AuthorFollower.author == author_id, Author.id != author_id)
            .all()
        ]
        await redis.execute("SET", f"author:followers:{author_id}", json.dumps(followers_ids))
        followers = await get_cached_authors_by_ids(followers_ids)
        return followers


# Get cached follower authors
async def get_cached_follower_authors(author_id: int):
    # Attempt to retrieve authors from cache
    cached = await redis.execute("get", f"author:follows-authors:{author_id}")
    if cached:
        authors_ids = json.loads(cached)
    else:
        # Query authors from database
        with local_session() as session:
            authors_ids = [
                a[0]
                for a in session.execute(
                    select(Author.id)
                    .select_from(join(Author, AuthorFollower, Author.id == AuthorFollower.author))
                    .where(AuthorFollower.follower == author_id)
                ).all()
            ]
            await redis.execute("SET", f"author:follows-authors:{author_id}", json.dumps(authors_ids))

    authors = await get_cached_authors_by_ids(authors_ids)
    return authors


# Get cached follower topics
async def get_cached_follower_topics(author_id: int):
    # Attempt to retrieve topics from cache
    cached = await redis.execute("get", f"author:follows-topics:{author_id}")
    if cached:
        topics_ids = json.loads(cached)
    else:
        # Load topics from database and cache them
        with local_session() as session:
            topics_ids = [
                t[0]
                for t in session.query(Topic.id)
                .join(TopicFollower, TopicFollower.topic == Topic.id)
                .where(TopicFollower.follower == author_id)
                .all()
            ]
            await redis.execute("SET", f"author:follows-topics:{author_id}", json.dumps(topics_ids))

    topics = []
    for topic_id in topics_ids:
        topic_str = await redis.execute("get", f"topic:id:{topic_id}")
        if topic_str:
            topic = json.loads(topic_str)
            if topic and topic not in topics:
                topics.append(topic)

    logger.debug(f"Cached topics for author#{author_id}: {len(topics)}")
    return topics


# Get author by user ID from cache
async def get_cached_author_by_user_id(user_id: str):
    """
    Retrieve author information by user_id, checking the cache first, then the database.

    Args:
        user_id (str): The user identifier for which to retrieve the author.

    Returns:
        dict: Dictionary with author data or None if not found.
    """
    # Attempt to find author ID by user_id in Redis cache
    author_id = await redis.execute("get", f"author:user:{user_id.strip()}")
    if author_id:
        # If ID is found, get full author data by ID
        author_data = await redis.execute("get", f"author:id:{author_id}")
        if author_data:
            return json.loads(author_data)

    # If data is not found in cache, query the database
    with local_session() as session:
        author = session.execute(select(Author).where(Author.user == user_id)).scalar_one_or_none()

        if author:
            # Cache the retrieved author data
            author_dict = author.dict()
            await asyncio.gather(
                redis.execute("SET", f"author:user:{user_id.strip()}", str(author.id)),
                redis.execute("SET", f"author:id:{author.id}", json.dumps(author_dict)),
            )
            return author_dict

    # Return None if author is not found
    return None


# Get cached topic authors
async def get_cached_topic_authors(topic_id: int):
    """
    Retrieve a list of authors for a given topic, using cache or database.

    Args:
        topic_id (int): The identifier of the topic for which to retrieve authors.

    Returns:
        List[dict]: A list of dictionaries containing author data.
    """
    # Attempt to get a list of author IDs from cache
    rkey = f"topic:authors:{topic_id}"
    cached_authors_ids = await redis.execute("get", rkey)
    if cached_authors_ids:
        authors_ids = json.loads(cached_authors_ids)
    else:
        # If cache is empty, get data from the database
        with local_session() as session:
            query = (
                select(ShoutAuthor.author)
                .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
                .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
                .where(and_(ShoutTopic.topic == topic_id, Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
            )
            authors_ids = [author_id for (author_id,) in session.execute(query).all()]
            # Cache the retrieved author IDs
            await redis.execute("set", rkey, json.dumps(authors_ids))

    # Retrieve full author details from cached IDs
    if authors_ids:
        authors = await get_cached_authors_by_ids(authors_ids)
        logger.debug(f"Topic#{topic_id} authors fetched and cached: {len(authors)} authors found.")
        return authors

    return []
