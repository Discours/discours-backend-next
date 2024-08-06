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


async def cache_multiple_items(items, cache_function):
    await asyncio.gather(*(cache_function(item) for item in items))


async def cache_topic(topic: dict):
    await cache_multiple_items([topic], _cache_topic_helper)


async def _cache_topic_helper(topic):
    topic_id = topic.get("id")
    topic_slug = topic.get("slug")
    payload = json.dumps(topic, cls=CustomJSONEncoder)
    await redis.set(f"topic:id:{topic_id}", payload)
    await redis.set(f"topic:slug:{topic_slug}", payload)


async def cache_author(author: dict):
    author_id = author.get("id")
    user_id = author.get("user", "").strip()
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.set(f"author:user:{user_id}", author_id)
    await redis.set(f"author:id:{author_id}", payload)


async def cache_follows(follower_id: int, entity_type: str, entity_id: int, is_insert=True):
    redis_key = f"author:follows-{entity_type}s:{follower_id}"
    follows = await redis.get(redis_key)
    follows = json.loads(follows) if follows else []

    if is_insert:
        follows.append(entity_id) if entity_id not in follows else None
    else:
        follows = [eid for eid in follows if eid != entity_id]

    payload = json.dumps(follows, cls=CustomJSONEncoder)
    await redis.set(redis_key, payload)
    follower = await redis.get(f"author:id:{follower_id}")
    if follower:
        follower = json.loads(follower)
        follower["stat"][f"{entity_type}s"] = len(follows)
        await cache_author(follower)


async def get_cached_topic_by_slug(slug: str, get_with_stat):
    cached_result = await redis.get(f"topic:slug:{slug}")
    if cached_result:
        return json.loads(cached_result)

    with local_session() as session:
        topic_query = select(Topic).filter(Topic.slug == slug)
        result = await get_with_stat(session.execute(topic_query))
        topic = result if isinstance(result, Topic) else result[0]
        if topic:
            await cache_topic(topic.dict())
            return topic


# Пример агрегации получения и кеширования информации для авторов
async def get_cached_authors_by_ids(author_ids: List[int]) -> List[dict]:
    cache_keys = [f"author:id:{author_id}" for author_id in author_ids]
    authors_data = await asyncio.gather(*(redis.get(key) for key in cache_keys))
    authors = [json.loads(author) for author in authors_data if author]

    # Кешируем отсутствующие данные
    missing_ids = [author_ids[i] for i, data in enumerate(authors_data) if not data]
    if missing_ids:
        with local_session() as session:
            query = select(Author).where(Author.id.in_(missing_ids))
            results = await session.execute(query)
            authors_to_cache = [result.dict() for result in results.scalars()]
            await cache_multiple_items(authors_to_cache, cache_author)
            authors.extend(authors_to_cache)

    return authors


# Остальные функции с аналогичными оптимизациями
