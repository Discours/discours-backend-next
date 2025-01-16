import asyncio

from cache.cache import cache_author, cache_topic, get_cached_author, get_cached_topic, get_cached_entity
from resolvers.stat import get_with_stat
from utils.logger import root_logger as logger


class CacheRevalidationManager:
    def __init__(self, interval=60):
        """Инициализация менеджера с заданным интервалом проверки (в секундах)."""
        self.interval = interval
        self.items_to_revalidate = {"authors": set(), "topics": set(), "shouts": set(), "reactions": set()}
        self.lock = asyncio.Lock()
        self.running = True

    async def start(self):
        """Запуск фонового воркера для ревалидации кэша."""
        self.task = asyncio.create_task(self.revalidate_cache())

    async def revalidate_cache(self):
        """Циклическая проверка и ревалидация кэша каждые self.interval секунд."""
        try:
            while self.running:
                await asyncio.sleep(self.interval)
                await self.process_revalidation()
        except asyncio.CancelledError:
            logger.info("Revalidation worker was stopped.")
        except Exception as e:
            logger.error(f"An error occurred in the revalidation worker: {e}")

    async def process_revalidation(self):
        """Обновление кэша для всех сущностей, требующих ревалидации."""
        async with self.lock:
            # Ревалидация кэша авторов
            for author_id in self.items_to_revalidate["authors"]:
                author = await get_cached_author(author_id, get_with_stat)
                if author:
                    await cache_author(author)
            self.items_to_revalidate["authors"].clear()

            # Ревалидация кэша тем
            for topic_id in self.items_to_revalidate["topics"]:
                topic = await get_cached_topic(topic_id)
                if topic:
                    await cache_topic(topic)
            self.items_to_revalidate["topics"].clear()

    def mark_for_revalidation(self, entity_id, entity_type):
        """Отметить сущность для ревалидации."""
        self.items_to_revalidate[entity_type].add(entity_id)

    async def stop(self):
        """Остановка фонового воркера."""
        self.running = False
        if hasattr(self, "task"):
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass


revalidation_manager = CacheRevalidationManager(interval=300)  # Ревалидация каждые 5 минут
