import asyncio
import json
import httpx
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
                # Use httpx to send a request to ElasticSearch
                async with httpx.AsyncClient() as client:
                    search_url = f"https://search.discours.io/search?q={text}"
                    response = await client.get(search_url)
                    if response.status_code == 200:
                        payload = response.json()
                        await redis.execute("SET", text, payload)
                        return json.loads(payload)
        else:
            return json.loads(cached)
