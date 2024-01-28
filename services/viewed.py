import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

# ga
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

from orm.author import Author
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from services.db import local_session


# Настройка журналирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('\t[services.viewed]\t')
logger.setLevel(logging.DEBUG)

GOOGLE_KEYFILE_PATH = os.environ.get('GOOGLE_KEYFILE_PATH', '/dump/google-service.json')
GOOGLE_PROPERTY_ID = os.environ.get('GOOGLE_PROPERTY_ID', '')
VIEWS_FILEPATH = '/dump/views.json'


class ViewedStorage:
    lock = asyncio.Lock()
    views_by_shout = {}
    shouts_by_topic = {}
    shouts_by_author = {}
    views = None
    period = 60 * 60  # каждый час
    analytics_client: BetaAnalyticsDataClient | None = None
    auth_result = None
    disabled = False
    start_date = int(time.time())

    @staticmethod
    async def init():
        """Подключение к клиенту Google Analytics с использованием аутентификации"""
        self = ViewedStorage
        async with self.lock:
            os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', GOOGLE_KEYFILE_PATH)
            if GOOGLE_KEYFILE_PATH:
                # Using a default constructor instructs the client to use the credentials
                # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
                self.analytics_client = BetaAnalyticsDataClient()
                logger.info(' * Клиент Google Analytics успешно авторизован')

                # Загрузка предварительно подсчитанных просмотров из файла JSON
                self.load_precounted_views()

                if os.path.exists(VIEWS_FILEPATH):
                    file_timestamp = os.path.getctime(VIEWS_FILEPATH)
                    self.start_date = datetime.fromtimestamp(file_timestamp).strftime('%Y-%m-%d')

                # Запуск фоновой задачи
                asyncio.create_task(self.worker())
            else:
                logger.info(' * Пожалуйста, добавьте ключевой файл Google Analytics')
                self.disabled = True

    @staticmethod
    def load_precounted_views():
        """Загрузка предварительно подсчитанных просмотров из файла JSON"""
        self = ViewedStorage
        try:
            with open(VIEWS_FILEPATH, 'r') as file:
                precounted_views = json.load(file)
                self.views_by_shout.update(precounted_views)
                logger.info(f' * {len(precounted_views)} публикаций с просмотрами успешно загружены.')
        except Exception as e:
            logger.error(f'Ошибка загрузки предварительно подсчитанных просмотров: {e}')

    @staticmethod
    async def update_pages():
        """Запрос всех страниц от Google Analytics, отсортированных по количеству просмотров"""
        self = ViewedStorage
        logger.info(' ⎧ Обновление данных просмотров от Google Analytics ---')
        if not self.disabled:
            try:
                start = time.time()
                async with self.lock:
                    if self.analytics_client:
                        request = RunReportRequest(
                            property=f'properties/{GOOGLE_PROPERTY_ID}',
                            dimensions=[Dimension(name='pagePath')],
                            metrics=[Metric(name='pageviews')],
                            date_ranges=[DateRange(start_date=self.start_date, end_date='today')],
                        )
                        response = self.analytics_client.run_report(request)
                        if response and isinstance(response.rows, list):
                            slugs = set()
                            for row in response.rows:
                                print(row.dimension_values[0].value, row.metric_values[0].value)
                                # Извлечение путей страниц из ответа Google Analytics
                                if isinstance(row.dimension_values, list):
                                    page_path = row.dimension_values[0].value
                                    slug = page_path.split('discours.io/')[-1]
                                    views_count = int(row.metric_values[0].value)

                                    # Обновление данных в хранилище
                                    self.views_by_shout[slug] = self.views_by_shout.get(slug, 0)
                                    self.views_by_shout[slug] += views_count
                                    self.update_topics(slug)

                                    # Запись путей страниц для логирования
                                    slugs.add(slug)

                                logger.info(f' ⎪ Собрано страниц: {len(slugs)} ')

                        end = time.time()
                        logger.info(' ⎪ Обновление страниц заняло %fs ' % (end - start))
            except Exception as error:
                logger.error(error)

    @staticmethod
    async def get_shout(shout_slug) -> int:
        """Получение метрики просмотров shout по slug"""
        self = ViewedStorage
        async with self.lock:
            return self.views_by_shout.get(shout_slug, 0)

    @staticmethod
    async def get_shout_media(shout_slug) -> Dict[str, int]:
        """Получение метрики воспроизведения shout по slug"""
        self = ViewedStorage
        async with self.lock:
            return self.views_by_shout.get(shout_slug, 0)

    @staticmethod
    async def get_topic(topic_slug) -> int:
        """Получение суммарного значения просмотров темы"""
        self = ViewedStorage
        topic_views = 0
        async with self.lock:
            for shout_slug in self.shouts_by_topic.get(topic_slug, []):
                topic_views += self.views_by_shout.get(shout_slug, 0)
        return topic_views

    @staticmethod
    async def get_author(author_slug) -> int:
        """Получение суммарного значения просмотров автора"""
        self = ViewedStorage
        author_views = 0
        async with self.lock:
            for shout_slug in self.shouts_by_author.get(author_slug, []):
                author_views += self.views_by_shout.get(shout_slug, 0)
        return author_views

    @staticmethod
    def update_topics(shout_slug):
        """Обновление счетчиков темы по slug shout"""
        self = ViewedStorage
        with local_session() as session:
            # Определение вспомогательной функции для избежания повторения кода
            def update_groups(dictionary, key, value):
                dictionary[key] = list(set(dictionary.get(key, []) + [value]))

            # Обновление тем и авторов с использованием вспомогательной функции
            for [_shout_topic, topic] in (
                session.query(ShoutTopic, Topic).join(Topic).join(Shout).where(Shout.slug == shout_slug).all()
            ):
                update_groups(self.shouts_by_topic, topic.slug, shout_slug)

            for [_shout_topic, author] in (
                session.query(ShoutAuthor, Author).join(Author).join(Shout).where(Shout.slug == shout_slug).all()
            ):
                update_groups(self.shouts_by_author, author.slug, shout_slug)

    @staticmethod
    async def worker():
        """Асинхронная задача обновления"""
        failed = 0
        self = ViewedStorage
        if self.disabled:
            return

        while True:
            try:
                await self.update_pages()
                failed = 0
            except Exception:
                failed += 1
                logger.info(' - Обновление не удалось #%d, ожидание 10 секунд' % failed)
                if failed > 3:
                    logger.info(' - Больше не пытаемся обновить')
                    break
            if failed == 0:
                when = datetime.now(timezone.utc) + timedelta(seconds=self.period)
                t = format(when.astimezone().isoformat())
                logger.info(' ⎩ Следующее обновление: %s' % (t.split('T')[0] + ' ' + t.split('T')[1].split('.')[0]))
                await asyncio.sleep(self.period)
            else:
                await asyncio.sleep(10)
                logger.info(' - Попытка снова обновить данные')
