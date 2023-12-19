import asyncio
import json
import logging
from typing import List

import aiohttp

from orm.shout import Shout  # Adjust the import as needed
from services.rediscache import redis  # Adjust the import as needed


class SearchService:
    lock = asyncio.Lock()

    @staticmethod
    async def init(session):
        async with SearchService.lock:
            logging.info("[services.search] Initializing SearchService")

    @staticmethod
    async def search(text: str, limit: int = 50, offset: int = 0) -> List[Shout]:
        try:
            payload = await redis.execute("GET", text)

            if not payload:
                async with SearchService.lock:
                    # Use aiohttp to send a request to ElasticSearch
                    async with aiohttp.ClientSession() as session:
                        search_url = f"https://search.discours.io/search?q={text}"
                        async with session.get(search_url) as response:
                            if response.status == 200:
                                payload = await response.json()
                                await redis.execute("SET", text, json.dumps(payload))  # use redis as cache
                            else:
                                logging.error(f"[services.search] response: {response.status}  {await response.text()}")
        except Exception as e:
            logging.error(f"[services.search] Error during search: {e}")
            payload = []

        return payload[offset : offset + limit]
