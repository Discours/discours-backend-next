import logging

from redis.asyncio import Redis

from settings import REDIS_URL

# Set redis logging level to suppress DEBUG messages
logger = logging.getLogger("redis")
logger.setLevel(logging.WARNING)


class RedisService:
    def __init__(self, uri=REDIS_URL):
        self._uri: str = uri
        self.pubsub_channels = []
        self._client = None

    async def connect(self):
        if self._uri:
            self._client = await Redis.from_url(self._uri, decode_responses=True)
            logger.info("Redis connection was established.")

    async def disconnect(self):
        if isinstance(self._client, Redis):
            await self._client.close()
            logger.info("Redis connection was closed.")

    async def execute(self, command, *args, **kwargs):
        if self._client:
            try:
                logger.debug(f"{command}")  # {args[0]}") # {args} {kwargs}")
                for arg in args:
                    if isinstance(arg, dict):
                        if arg.get("_sa_instance_state"):
                            del arg["_sa_instance_state"]
                r = await self._client.execute_command(command, *args, **kwargs)
                # logger.debug(type(r))
                # logger.debug(r)
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

    async def set(self, key, data, ex=None):
        # Prepare the command arguments
        args = [key, data]

        # If an expiration time is provided, add it to the arguments
        if ex is not None:
            args.append("EX")
            args.append(ex)

        # Execute the command with the provided arguments
        await self.execute("set", *args)

    async def get(self, key):
        return await self.execute("get", key)


redis = RedisService()

__all__ = ["redis"]
