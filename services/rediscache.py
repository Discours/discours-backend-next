import redis.asyncio as aredis
from settings import REDIS_URL

class RedisCache:
    def __init__(self, uri=REDIS_URL):
        self._uri: str = uri
        self.pubsub_channels = []
        self._client = None

    async def connect(self):
        self._client = aredis.Redis.from_url(self._uri, decode_responses=True)

    async def disconnect(self):
        await self._client.aclose()

    async def execute(self, command, *args, **kwargs):
        if not self._client:
            await self.connect()
        try:
            print(f"[redis] {command} {args}")
            return await self._client.execute_command(command, *args, **kwargs)
        except Exception as e:
            print(f"[redis] ERROR: {e} with: {command} {args}")
            import traceback

            traceback.print_exc()
            return None

    async def subscribe(self, *channels):
        if not self._client:
            await self.connect()
        async with self._client.pubsub() as pubsub:
            for channel in channels:
                await pubsub.subscribe(channel)
                self.pubsub_channels.append(channel)

    async def unsubscribe(self, *channels):
        if not self._client:
            return
        async with self._client.pubsub() as pubsub:
            for channel in channels:
                await pubsub.unsubscribe(channel)
                self.pubsub_channels.remove(channel)

    async def publish(self, channel, data):
        if not self._client:
            return
        await self._client.publish(channel, data)

    async def lrange(self, key, start, stop):
        print(f"[redis] LRANGE {key} {start} {stop}")
        return await self._client.lrange(key, start, stop)

    async def mget(self, key, *keys):
        print(f"[redis] MGET {key} {keys}")
        return await self._client.mget(key, *keys)

redis = RedisCache()

__all__ = ["redis"]
