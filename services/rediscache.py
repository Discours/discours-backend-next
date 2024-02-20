import redis.asyncio as aredis

from services.logger import get_colorful_logger
from settings import REDIS_URL

logger = get_colorful_logger('services.redis')


class RedisCache:
    def __init__(self, uri=REDIS_URL):
        self._uri: str = uri
        self.pubsub_channels = []
        self._client = None

    async def connect(self):
        self._client = aredis.Redis.from_url(self._uri, decode_responses=True)

    async def disconnect(self):
        if self._client:
            await self._client.close()

    async def execute(self, command, *args, **kwargs):
        if self._client:
            try:
                logger.debug(f'{command} {args} {kwargs}')
                r = await self._client.execute_command(command, *args, **kwargs)
                logger.debug(type(r))
                logger.debug(r)
                return r
            except Exception as e:
                logger.error(e)

    async def subscribe(self, *channels):
        if self._client:
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


redis = RedisCache()

__all__ = ['redis']
