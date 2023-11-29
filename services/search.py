import asyncio
import json
import aiohttp
from services.rediscache import redis
from orm.shout import Shout


class SearchService:
    lock = asyncio.Lock()
    cache = {}

    @staticmethod
    async def init(session):
        async with SearchService.lock:
            print("[services.search] did nothing")
            SearchService.cache = {}

    @staticmethod
    async def search(text, limit, offset) -> [Shout]:
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
                            return payload
        else:
            return json.loads(cached)
