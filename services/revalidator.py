import asyncio
from services.logger import root_logger as logger
from services.cache import get_cached_author, cache_author, cache_topic, get_cached_topic


class CacheRevalidationManager:
    """Управление периодической ревалидацией кэша."""

    def __init__(self):
        self.items_to_revalidate = {"authors": set(), "topics": set()}
        self.revalidation_interval = 60  # Интервал ревалидации в секундах
        self.loop = None

    def start(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.revalidate_cache())
        self.loop.run_forever()
        logger.info("[services.revalidator] started infinite loop")

    async def revalidate_cache(self):
        """Периодическая ревалидация кэша."""
        while True:
            await asyncio.sleep(self.revalidation_interval)
            await self.process_revalidation()

    async def process_revalidation(self):
        """Ревалидация кэша для отмеченных сущностей."""
        for entity_type, ids in self.items_to_revalidate.items():
            for entity_id in ids:
                if entity_type == "authors":
                    # Ревалидация кэша автора
                    author = await get_cached_author(entity_id)
                    if author:
                        await cache_author(author)
                elif entity_type == "topics":
                    # Ревалидация кэша темы
                    topic = await get_cached_topic(entity_id)
                    if topic:
                        await cache_topic(topic)
            ids.clear()

    def mark_for_revalidation(self, entity_id, entity_type):
        """Отметить сущность для ревалидации."""
        self.items_to_revalidate[entity_type].add(entity_id)


# Инициализация менеджера ревалидации
revalidation_manager = CacheRevalidationManager()
