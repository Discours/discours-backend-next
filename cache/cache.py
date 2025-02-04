import asyncio
import json
from typing import List

from sqlalchemy import and_, join, select

from orm.author import Author, AuthorFollower
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.db import local_session
from services.redis import redis
from utils.encoders import CustomJSONEncoder
from utils.logger import root_logger as logger

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "shouts": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}

CACHE_TTL = 300  # 5 минут

CACHE_KEYS = {
    "TOPIC_ID": "topic:id:{}",
    "TOPIC_SLUG": "topic:slug:{}",
    "TOPIC_AUTHORS": "topic:authors:{}",
    "TOPIC_FOLLOWERS": "topic:followers:{}",
    "TOPIC_SHOUTS": "topic_shouts_{}",
    "AUTHOR_ID": "author:id:{}",
    "AUTHOR_USER": "author:user:{}",
    "SHOUTS": "shouts:{}",
}


# Cache topic data
async def cache_topic(topic: dict):
    payload = json.dumps(topic, cls=CustomJSONEncoder)
    await asyncio.gather(
        redis_operation("SET", f"topic:id:{topic['id']}", payload),
        redis_operation("SET", f"topic:slug:{topic['slug']}", payload),
    )


# Cache author data
async def cache_author(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await asyncio.gather(
        redis_operation("SET", f"author:user:{author['user'].strip()}", str(author["id"])),
        redis_operation("SET", f"author:id:{author['id']}", payload),
    )


# Cache follows data
async def cache_follows(follower_id: int, entity_type: str, entity_id: int, is_insert=True):
    key = f"author:follows-{entity_type}s:{follower_id}"
    follows_str = await redis_operation("GET", key)
    follows = json.loads(follows_str) if follows_str else DEFAULT_FOLLOWS[entity_type]
    if is_insert:
        if entity_id not in follows:
            follows.append(entity_id)
    else:
        follows = [eid for eid in follows if eid != entity_id]
    await redis_operation("SET", key, json.dumps(follows, cls=CustomJSONEncoder))
    await update_follower_stat(follower_id, entity_type, len(follows))


# Update follower statistics
async def update_follower_stat(follower_id, entity_type, count):
    follower_key = f"author:id:{follower_id}"
    follower_str = await redis_operation("GET", follower_key)
    follower = json.loads(follower_str) if follower_str else None
    if follower:
        follower["stat"] = {f"{entity_type}s": count}
        await cache_author(follower)


# Get author from cache
async def get_cached_author(author_id: int, get_with_stat):
    author_key = f"author:id:{author_id}"
    result = await redis_operation("GET", author_key)
    if result:
        return json.loads(result)
    # Load from database if not found in cache
    q = select(Author).where(Author.id == author_id)
    authors = get_with_stat(q)
    if authors:
        author = authors[0]
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
    cached_topic = await redis_operation("GET", topic_key)
    if cached_topic:
        return json.loads(cached_topic)

    # If not in cache, fetch from the database
    with local_session() as session:
        topic = session.execute(select(Topic).where(Topic.id == topic_id)).scalar_one_or_none()
        if topic:
            topic_dict = topic.dict()
            await redis_operation("SET", topic_key, json.dumps(topic_dict, cls=CustomJSONEncoder))
            return topic_dict

    return None


# Get topic by slug from cache
async def get_cached_topic_by_slug(slug: str, get_with_stat):
    topic_key = f"topic:slug:{slug}"
    result = await redis_operation("GET", topic_key)
    if result:
        return json.loads(result)
    # Load from database if not found in cache
    topic_query = select(Topic).where(Topic.slug == slug)
    topics = get_with_stat(topic_query)
    if topics:
        topic_dict = topics[0].dict()
        await cache_topic(topic_dict)
        return topic_dict
    return None


# Get list of authors by ID from cache
async def get_cached_authors_by_ids(author_ids: List[int]) -> List[dict]:
    # Fetch all author data concurrently
    keys = [f"author:id:{author_id}" for author_id in author_ids]
    results = await asyncio.gather(*(redis_operation("GET", key) for key in keys))
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
    
    Args:
        topic_id: ID темы
        
    Returns:
        List[dict]: Список подписчиков с их данными
    """
    try:
        cache_key = CACHE_KEYS["TOPIC_FOLLOWERS"].format(topic_id)
        cached = await redis_operation("GET", cache_key)
        
        if cached:
            followers_ids = json.loads(cached)
            logger.debug(f"Found {len(followers_ids)} cached followers for topic #{topic_id}")
            return await get_cached_authors_by_ids(followers_ids)

        with local_session() as session:
            followers_ids = [
                f[0] for f in session.query(Author.id)
                .join(TopicFollower, TopicFollower.follower == Author.id)
                .filter(TopicFollower.topic == topic_id)
                .all()
            ]
            
            await redis_operation("SETEX", cache_key, value=json.dumps(followers_ids), ttl=CACHE_TTL)
            followers = await get_cached_authors_by_ids(followers_ids)
            logger.debug(f"Cached {len(followers)} followers for topic #{topic_id}")
            return followers

    except Exception as e:
        logger.error(f"Error getting followers for topic #{topic_id}: {str(e)}")
        return []


# Get cached author followers
async def get_cached_author_followers(author_id: int):
    # Check cache for data
    cached = await redis_operation("GET", f"author:followers:{author_id}")
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
        await redis_operation("SET", f"author:followers:{author_id}", json.dumps(followers_ids))
        followers = await get_cached_authors_by_ids(followers_ids)
        return followers


# Get cached follower authors
async def get_cached_follower_authors(author_id: int):
    # Attempt to retrieve authors from cache
    cached = await redis_operation("GET", f"author:follows-authors:{author_id}")
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
            await redis_operation("SET", f"author:follows-authors:{author_id}", json.dumps(authors_ids))

    authors = await get_cached_authors_by_ids(authors_ids)
    return authors


# Get cached follower topics
async def get_cached_follower_topics(author_id: int):
    # Attempt to retrieve topics from cache
    cached = await redis_operation("GET", f"author:follows-topics:{author_id}")
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
            await redis_operation("SET", f"author:follows-topics:{author_id}", json.dumps(topics_ids))

    topics = []
    for topic_id in topics_ids:
        topic_str = await redis_operation("GET", f"topic:id:{topic_id}")
        if topic_str:
            topic = json.loads(topic_str)
            if topic and topic not in topics:
                topics.append(topic)

    logger.debug(f"Cached topics for author#{author_id}: {len(topics)}")
    return topics


# Get author by user ID from cache
async def get_cached_author_by_user_id(user_id: str, get_with_stat):
    """
    Retrieve author information by user_id, checking the cache first, then the database.

    Args:
        user_id (str): The user identifier for which to retrieve the author.

    Returns:
        dict: Dictionary with author data or None if not found.
    """
    # Attempt to find author ID by user_id in Redis cache
    author_id = await redis_operation("GET", f"author:user:{user_id.strip()}")
    if author_id:
        # If ID is found, get full author data by ID
        author_data = await redis_operation("GET", f"author:id:{author_id}")
        if author_data:
            return json.loads(author_data)

    # If data is not found in cache, query the database
    author_query = select(Author).where(Author.user == user_id)
    authors = get_with_stat(author_query)
    if authors:
        # Cache the retrieved author data
        author = authors[0]
        author_dict = author.dict()
        await asyncio.gather(
            redis_operation("SET", f"author:user:{user_id.strip()}", str(author.id)),
            redis_operation("SET", f"author:id:{author.id}", json.dumps(author_dict)),
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
    cached_authors_ids = await redis_operation("GET", rkey)
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
            await redis_operation("SET", rkey, json.dumps(authors_ids))

    # Retrieve full author details from cached IDs
    if authors_ids:
        authors = await get_cached_authors_by_ids(authors_ids)
        logger.debug(f"Topic#{topic_id} authors fetched and cached: {len(authors)} authors found.")
        return authors

    return []


async def invalidate_shouts_cache(cache_keys: List[str]):
    """
    Инвалидирует кэш выборок публикаций по переданным ключам.
    """
    for key in cache_keys:
        try:
            # Формируем полный ключ кэша
            cache_key = f"shouts:{key}"

            # Удаляем основной кэш
            await redis_operation("DEL", cache_key)
            logger.debug(f"Invalidated cache key: {cache_key}")

            # Добавляем ключ в список инвалидированных с TTL
            await redis_operation("SETEX", f"{cache_key}:invalidated", value="1", ttl=CACHE_TTL)

            # Если это кэш темы, инвалидируем также связанные ключи
            if key.startswith("topic_"):
                topic_id = key.split("_")[1]
                related_keys = [
                    f"topic:id:{topic_id}",
                    f"topic:authors:{topic_id}",
                    f"topic:followers:{topic_id}",
                    f"topic:stats:{topic_id}",
                ]
                for related_key in related_keys:
                    await redis_operation("DEL", related_key)
                    logger.debug(f"Invalidated related key: {related_key}")

        except Exception as e:
            logger.error(f"Error invalidating cache key {key}: {e}")


async def cache_topic_shouts(topic_id: int, shouts: List[dict]):
    """Кэширует список публикаций для темы"""
    key = f"topic_shouts_{topic_id}"
    payload = json.dumps(shouts, cls=CustomJSONEncoder)
    await redis_operation("SETEX", key, value=payload, ttl=CACHE_TTL)


async def get_cached_topic_shouts(topic_id: int) -> List[dict]:
    """Получает кэшированный список публикаций для темы"""
    key = f"topic_shouts_{topic_id}"
    cached = await redis_operation("GET", key)
    if cached:
        return json.loads(cached)
    return None


async def cache_related_entities(shout: Shout):
    """
    Кэширует все связанные с публикацией сущности (авторов и темы)
    """
    tasks = []
    for author in shout.authors:
        tasks.append(cache_by_id(Author, author.id, cache_author))
    for topic in shout.topics:
        tasks.append(cache_by_id(Topic, topic.id, cache_topic))
    await asyncio.gather(*tasks)


async def invalidate_shout_related_cache(shout: Shout, author_id: int):
    """
    Инвалидирует весь кэш, связанный с публикацией и её связями
    
    Args:
        shout: Объект публикации
        author_id: ID автора
    """
    cache_keys = {
        "feed",  # основная лента
        f"author_{author_id}",  # публикации автора
        "random_top",  # случайные топовые
        "unrated",  # неоцененные
        "recent",  # последние
        "coauthored",  # совместные
    }
    
    # Добавляем ключи авторов
    cache_keys.update(
        f"author_{a.id}" for a in shout.authors
    )
    cache_keys.update(
        f"authored_{a.id}" for a in shout.authors
    )
    
    # Добавляем ключи тем
    cache_keys.update(
        f"topic_{t.id}" for t in shout.topics
    )
    cache_keys.update(
        f"topic_shouts_{t.id}" for t in shout.topics
    )

    await invalidate_shouts_cache(list(cache_keys))


async def redis_operation(operation: str, key: str, value=None, ttl=None):
    """
    Унифицированная функция для работы с Redis

    Args:
        operation: 'GET', 'SET', 'DEL', 'SETEX'
        key: ключ
        value: значение (для SET/SETEX)
        ttl: время жизни в секундах (для SETEX)
    """
    try:
        if operation == "GET":
            return await redis.execute("GET", key)
        elif operation == "SET":
            await redis.execute("SET", key, value)
        elif operation == "SETEX":
            await redis.execute("SETEX", key, ttl or CACHE_TTL, value)
        elif operation == "DEL":
            await redis.execute("DEL", key)
    except Exception as e:
        logger.error(f"Redis {operation} error for key {key}: {e}")


async def get_cached_entity(entity_type: str, entity_id: int, get_method, cache_method):
    """
    Универсальная функция получения кэшированной сущности

    Args:
        entity_type: 'author' или 'topic'
        entity_id: ID сущности
        get_method: метод получения из БД
        cache_method: метод кэширования
    """
    key = f"{entity_type}:id:{entity_id}"
    cached = await redis_operation("GET", key)
    if cached:
        return json.loads(cached)

    entity = await get_method(entity_id)
    if entity:
        await cache_method(entity)
        return entity
    return None


async def cache_by_id(entity, entity_id: int, cache_method):
    """
    Кэширует сущность по ID, используя указанный метод кэширования

    Args:
        entity: класс сущности (Author/Topic)
        entity_id: ID сущности
        cache_method: функция кэширования
    """
    from resolvers.stat import get_with_stat

    caching_query = select(entity).filter(entity.id == entity_id)
    result = get_with_stat(caching_query)
    if not result or not result[0]:
        logger.warning(f"{entity.__name__} with id {entity_id} not found")
        return
    x = result[0]
    d = x.dict()
    await cache_method(d)
    return d
