import asyncio
import json
from typing import List

from sqlalchemy import select, join, and_
from orm.author import Author, AuthorFollower
from orm.topic import Topic, TopicFollower
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.rediscache import redis
from services.logger import root_logger as logger

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


# Кэширование данных темы
async def cache_topic(topic: dict):
    payload = json.dumps(topic, cls=CustomJSONEncoder)
    # Одновременное кэширование по id и slug для быстрого доступа
    await asyncio.gather(
        redis.execute("SET", f"topic:id:{topic['id']}", payload),
        redis.execute("SET", f"topic:slug:{topic['slug']}", payload),
    )


# Кэширование данных автора
async def cache_author(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    # Кэширование данных автора по user и id
    await asyncio.gather(
        redis.execute("SET", f"author:user:{author['user'].strip()}", str(author["id"])),
        redis.execute("SET", f"author:id:{author['id']}", payload),
    )


async def get_cached_topic(topic_id: int):
    """
    Получает информацию о теме из кэша или базы данных.

    Args:
        topic_id (int): Идентификатор темы.

    Returns:
        dict: Данные темы в формате словаря или None, если тема не найдена.
    """
    # Ключ для кэширования темы в Redis
    topic_key = f"topic:id:{topic_id}"
    cached_topic = await redis.get(topic_key)
    if cached_topic:
        return json.loads(cached_topic)

    # Если данных о теме нет в кэше, загружаем из базы данных
    with local_session() as session:
        topic = session.execute(select(Topic).where(Topic.id == topic_id)).scalar_one_or_none()
        if topic:
            # Кэшируем полученные данные
            topic_dict = topic.dict()
            await redis.set(topic_key, json.dumps(topic_dict, cls=CustomJSONEncoder))
            return topic_dict

    return None


async def get_cached_shout_authors(shout_id: int):
    """
    Retrieves a list of authors for a given shout from the cache or database if not present.

    Args:
        shout_id (int): The ID of the shout for which to retrieve authors.

    Returns:
        List[dict]: A list of dictionaries containing author data.
    """
    # Attempt to retrieve cached author IDs for the shout
    rkey = f"shout:authors:{shout_id}"
    cached_author_ids = await redis.get(rkey)
    if cached_author_ids:
        author_ids = json.loads(cached_author_ids)
    else:
        # If not in cache, fetch from the database and cache the result
        with local_session() as session:
            query = (
                select(ShoutAuthor.author)
                .where(ShoutAuthor.shout == shout_id)
                .join(Author, ShoutAuthor.author == Author.id)
                .filter(Author.deleted_at.is_(None))
            )
            author_ids = [author_id for (author_id,) in session.execute(query).all()]
            await redis.execute("set", rkey, json.dumps(author_ids))

    # Retrieve full author details from cached IDs
    if author_ids:
        authors = await get_cached_authors_by_ids(author_ids)
        logger.debug(f"Shout#{shout_id} authors fetched and cached: {len(authors)} authors found.")
        return authors

    return []


# Кэширование данных о подписках
async def cache_follows(follower_id: int, entity_type: str, entity_id: int, is_insert=True):
    key = f"author:follows-{entity_type}s:{follower_id}"
    follows_str = await redis.get(key)
    follows = json.loads(follows_str) if follows_str else []
    if is_insert:
        if entity_id not in follows:
            follows.append(entity_id)
    else:
        follows = [eid for eid in follows if eid != entity_id]
    await redis.execute("set", key, json.dumps(follows, cls=CustomJSONEncoder))
    update_follower_stat(follower_id, entity_type, len(follows))


# Обновление статистики подписчика
async def update_follower_stat(follower_id, entity_type, count):
    follower_key = f"author:id:{follower_id}"
    follower_str = await redis.get(follower_key)
    follower = json.loads(follower_str) if follower_str else None
    if follower:
        follower["stat"] = {f"{entity_type}s": count}
        await cache_author(follower)


# Получение автора из кэша
async def get_cached_author(author_id: int):
    author_key = f"author:id:{author_id}"
    result = await redis.get(author_key)
    if result:
        return json.loads(result)
    # Загрузка из базы данных, если не найдено в кэше
    with local_session() as session:
        author = session.execute(select(Author).where(Author.id == author_id)).scalar_one_or_none()
        if author:
            await cache_author(author.dict())
            return author.dict()
    return None


# Получение темы по slug из кэша
async def get_cached_topic_by_slug(slug: str):
    topic_key = f"topic:slug:{slug}"
    result = await redis.get(topic_key)
    if result:
        return json.loads(result)
    # Загрузка из базы данных, если не найдено в кэше
    with local_session() as session:
        topic = session.execute(select(Topic).where(Topic.slug == slug)).scalar_one_or_none()
        if topic:
            await cache_topic(topic.dict())
            return topic.dict()
    return None


# Получение списка авторов по ID из кэша
async def get_cached_authors_by_ids(author_ids: List[int]) -> List[dict]:
    # Одновременное получение данных всех авторов
    keys = [f"author:id:{author_id}" for author_id in author_ids]
    results = await asyncio.gather(*(redis.get(key) for key in keys))
    authors = [json.loads(result) if result else None for result in results]
    # Загрузка отсутствующих авторов из базы данных и кэширование
    missing_indices = [index for index, author in enumerate(authors) if author is None]
    if missing_indices:
        missing_ids = [author_ids[index] for index in missing_indices]
        with local_session() as session:
            query = select(Author).where(Author.id.in_(missing_ids))
            missing_authors = session.execute(query).scalars().all()
            await asyncio.gather(*(cache_author(author.dict()) for author in missing_authors))
            authors = [author.dict() for author in missing_authors]
    return authors


async def get_cached_topic_followers(topic_id: int):
    # Попытка извлечь кэшированные данные
    cached = await redis.get(f"topic:followers:{topic_id}")
    if cached:
        followers = json.loads(cached)
        logger.debug(f"Cached followers for topic#{topic_id}: {len(followers)}")
        return followers

    # Загрузка из базы данных и кэширование результатов
    with local_session() as session:
        followers_ids = [
            f[0]
            for f in session.query(Author.id)
            .join(TopicFollower, TopicFollower.follower == Author.id)
            .filter(TopicFollower.topic == topic_id)
            .all()
        ]
        await redis.execute("SET", f"topic:followers:{topic_id}", json.dumps(followers_ids))
        followers = await get_cached_authors_by_ids(followers_ids)
        return followers


async def get_cached_author_followers(author_id: int):
    # Проверяем кэш на наличие данных
    cached = await redis.get(f"author:followers:{author_id}")
    if cached:
        followers_ids = json.loads(cached)
        followers = await get_cached_authors_by_ids(followers_ids)
        logger.debug(f"Cached followers for author#{author_id}: {len(followers)}")
        return followers

    # Запрос в базу данных если кэш пуст
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


async def get_cached_follower_authors(author_id: int):
    # Попытка получить авторов из кэша
    cached = await redis.get(f"author:follows-authors:{author_id}")
    if cached:
        authors_ids = json.loads(cached)
    else:
        # Запрос авторов из базы данных
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


async def get_cached_follower_topics(author_id: int):
    # Попытка получить темы из кэша
    cached = await redis.get(f"author:follows-topics:{author_id}")
    if cached:
        topics_ids = json.loads(cached)
    else:
        # Загрузка тем из базы данных и их кэширование
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
        topic_str = await redis.get(f"topic:id:{topic_id}")
        if topic_str:
            topic = json.loads(topic_str)
            if topic and topic not in topics:
                topics.append(topic)

    logger.debug(f"Cached topics for author#{author_id}: {len(topics)}")
    return topics


async def get_cached_author_by_user_id(user_id: str):
    """
    Получает информацию об авторе по его user_id, сначала проверяя кэш, а затем базу данных.

    Args:
        user_id (str): Идентификатор пользователя, по которому нужно получить автора.

    Returns:
        dict: Словарь с данными автора или None, если автор не найден.
    """
    # Пытаемся найти ID автора по user_id в кэше Redis
    author_id = await redis.get(f"author:user:{user_id.strip()}")
    if author_id:
        # Если ID найден, получаем полные данные автора по его ID
        author_data = await redis.get(f"author:id:{author_id}")
        if author_data:
            return json.loads(author_data)

    # Если данные в кэше не найдены, выполняем запрос к базе данных
    with local_session() as session:
        author = session.execute(select(Author).where(Author.user == user_id)).scalar_one_or_none()

        if author:
            # Кэшируем полученные данные автора
            author_dict = author.dict()
            await asyncio.gather(
                redis.execute("SET", f"author:user:{user_id.strip()}", str(author.id)),
                redis.execute("SET", f"author:id:{author.id}", json.dumps(author_dict)),
            )
            return author_dict

    # Возвращаем None, если автор не найден
    return None


async def get_cached_topic_authors(topic_id: int):
    """
    Получает список авторов для заданной темы, используя кэш или базу данных.

    Args:
        topic_id (int): Идентификатор темы, для которой нужно получить авторов.

    Returns:
        List[dict]: Список словарей, содержащих данные авторов.
    """
    # Пытаемся получить список ID авторов из кэша
    rkey = f"topic:authors:{topic_id}"
    cached_authors_ids = await redis.get(rkey)
    if cached_authors_ids:
        authors_ids = json.loads(cached_authors_ids)
    else:
        # Если кэш пуст, получаем данные из базы данных
        with local_session() as session:
            query = (
                select(ShoutAuthor.author)
                .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
                .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
                .where(and_(ShoutTopic.topic == topic_id, Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
            )
            authors_ids = [author_id for (author_id,) in session.execute(query).all()]
            # Кэшируем полученные ID авторов
            await redis.execute("set", rkey, json.dumps(authors_ids))

    # Получаем полные данные авторов по кэшированным ID
    if authors_ids:
        authors = await get_cached_authors_by_ids(authors_ids)
        logger.debug(f"Topic#{topic_id} authors fetched and cached: {len(authors)} authors found.")
        return authors

    return []
