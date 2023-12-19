import asyncio
import json
from typing import List

import aiohttp

from orm.shout import Shout
from services.rediscache import redis


class SearchService:
    lock = asyncio.Lock()
    cache = {}

    @staticmethod
    async def init(session):
        async with SearchService.lock:
            print("[services.search] did nothing")
            SearchService.cache = {}

    @staticmethod
    async def search(text, limit: int = 50, offset: int = 0) -> List[Shout]:
        cached = await redis.execute("GET", text)
        if not cached:
            async with SearchService.lock:
                # Use aiohttp to send a request to ElasticSearch
                async with aiohttp.ClientSession() as session:
                    search_url = f"https://search.discours.io/search?q={text}"
                    async with session.get(search_url) as response:
                        if response.status == 200:
                            payload = await response.json()
                            await redis.execute("SET", text, json.dumps(payload))
                            return payload[offset : offset + limit]
                        else:
                            print(f"[services.search] response: {response.status} {response.text}")
        else:
            return json.loads(cached)[offset : offset + limit]
