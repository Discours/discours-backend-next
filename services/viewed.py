import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

# ga
from apiclient.discovery import build
from google.oauth2.service_account import Credentials

from orm.author import Author
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from services.db import local_session


# Настройка журналирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('\t[services.viewed]\t')
logger.setLevel(logging.DEBUG)

GOOGLE_KEYFILE_PATH = os.environ.get('GOOGLE_KEYFILE_PATH', '/dump/google-service.json')
# GOOGLE_ANALYTICS_API = 'https://analyticsreporting.googleapis.com/v4'
GOOGLE_ANALYTICS_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']


# Функция для создания объекта службы Analytics Reporting API V4
def get_service():
    credentials = Credentials.from_service_account_file(GOOGLE_KEYFILE_PATH, scopes=GOOGLE_ANALYTICS_SCOPES)
    service = build(serviceName='analyticsreporting', version='v4', credentials=credentials)
    return service


class ViewedStorage:
    lock = asyncio.Lock()
    views_by_shout = {}
    shouts_by_topic = {}
    shouts_by_author = {}
    views = None
    period = 60 * 60  # каждый час
    analytics_client = None
    auth_result = None
    disabled = False
    days_ago = 0

    @staticmethod
    async def init():
        """Подключение к клиенту Google Analytics с использованием аутентификации"""
        self = ViewedStorage
        async with self.lock:
            if os.path.exists(GOOGLE_KEYFILE_PATH):
                self.analytics_client = get_service()
                logger.info(f' * Постоянная авторизация в Google Analytics {self.analytics_client}')

                # Загрузка предварительно подсчитанных просмотров из файла JSON
                self.load_precounted_views()

                file_path = '/dump/views.json'
                if os.path.exists(file_path):
                    creation_time = os.path.getctime(file_path)
                    current_time = datetime.now().timestamp()
                    time_difference_seconds = current_time - creation_time
                    self.days_ago = int(time_difference_seconds / (24 * 3600))  # Convert seconds to days
                    logger.info(f'The file {file_path} was created {self. days_ago} days ago.')
                else:
                    logger.info(f'The file {file_path} does not exist.')

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
            with open('/dump/views.json', 'r') as file:
                precounted_views = json.load(file)
                self.views_by_shout.update(precounted_views)
                logger.info(
                    f' * {len(precounted_views)} публикаций с просмотрами успешно загружены.'
                )
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
                        data = (
                            self.analytics_client.data().batchRunReports(
                                {
                                    'requests': [
                                        {
                                            'dateRanges': [{'startDate': f'{self.days_ago}daysAgo', 'endDate': 'today'}],
                                            'metrics': [{'expression': 'ga:pageviews'}],
                                            'dimensions': [{'name': 'ga:pagePath'}],
                                        }
                                    ]
                                }
                            )
                            .execute()
                        )
                        if isinstance(data, dict):
                            slugs = set()
                            reports = data.get('reports', [])
                            if reports and isinstance(reports, list):
                                rows = list(reports[0].get('data', {}).get('rows', []))
                                for row in rows:
                                    # Извлечение путей страниц из ответа Google Analytics
                                    if isinstance(row, dict):
                                        dimensions = row.get('dimensions', [])
                                        if isinstance(dimensions, list) and dimensions:
                                            page_path = dimensions[0]
                                            slug = page_path.split('discours.io/')[-1]
                                            views_count = int(row['metrics'][0]['values'][0])

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
