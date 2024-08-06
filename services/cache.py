import asyncio
import json
from typing import List

from sqlalchemy import select
from orm.author import Author
from orm.topic import Topic
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.rediscache import redis

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
        redis.set(f"topic:id:{topic['id']}", payload), redis.set(f"topic:slug:{topic['slug']}", payload)
    )


# Кэширование данных автора
async def cache_author(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    # Кэширование данных автора по user и id
    await asyncio.gather(
        redis.set(f"author:user:{author['user'].strip()}", str(author["id"])),
        redis.set(f"author:id:{author['id']}", payload),
    )


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
    await redis.set(key, json.dumps(follows, cls=CustomJSONEncoder))
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
