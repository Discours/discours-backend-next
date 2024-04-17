from dogpile.cache import make_region

from settings import REDIS_URL

# Создание региона кэша с TTL
cache_region = make_region()
cache_region.configure(
    "dogpile.cache.redis",
    arguments={"url": f"{REDIS_URL}/1"},
    expiration_time=3600,  # Cache expiration time in seconds
)
