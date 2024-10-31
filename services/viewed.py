import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

# ga
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

from orm.author import Author
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from services.db import local_session
from utils.logger import root_logger as logger

GOOGLE_KEYFILE_PATH = os.environ.get("GOOGLE_KEYFILE_PATH", "/dump/google-service.json")
GOOGLE_PROPERTY_ID = os.environ.get("GOOGLE_PROPERTY_ID", "")
VIEWS_FILEPATH = "/dump/views.json"


class ViewedStorage:
    lock = asyncio.Lock()
    views_by_shout_slug = {}
    views_by_shout_id = {}
    shouts_by_topic = {}
    shouts_by_author = {}
    views = None
    period = 60 * 60  # каждый час
    analytics_client: BetaAnalyticsDataClient | None = None
    auth_result = None
    disabled = False
    start_date = datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    async def init():
        """Подключение к клиенту Google Analytics с использованием аутентификации"""
        self = ViewedStorage
        async with self.lock:
            # Загрузка предварительно подсчитанных просмотров из файла JSON
            self.load_precounted_views()

            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", GOOGLE_KEYFILE_PATH)
            if GOOGLE_KEYFILE_PATH and os.path.isfile(GOOGLE_KEYFILE_PATH):
                # Using a default constructor instructs the client to use the credentials
                # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
                self.analytics_client = BetaAnalyticsDataClient()
                logger.info(" * Google Analytics credentials accepted")

                # Запуск фоновой задачи
                _task = asyncio.create_task(self.worker())
            else:
                logger.warning(" * please, add Google Analytics credentials file")
                self.disabled = True

    @staticmethod
    def load_precounted_views():
        """Загрузка предварительно подсчитанных просмотров из файла JSON"""
        self = ViewedStorage
        viewfile_path = VIEWS_FILEPATH
        if not os.path.exists(viewfile_path):
            viewfile_path = os.path.join(os.path.curdir, "views.json")
            if not os.path.exists(viewfile_path):
                logger.warning(" * views.json not found")
                return

        logger.info(f" * loading views from {viewfile_path}")
        try:
            start_date_int = os.path.getmtime(viewfile_path)
            start_date_str = datetime.fromtimestamp(start_date_int).strftime("%Y-%m-%d")
            self.start_date = start_date_str
            now_date = datetime.now().strftime("%Y-%m-%d")

            if now_date == self.start_date:
                logger.info(" * views data is up to date!")
            else:
                logger.warn(f" * {viewfile_path} is too old: {self.start_date}")

            with open(viewfile_path, "r") as file:
                precounted_views = json.load(file)
                self.views_by_shout_slug.update(precounted_views)
                logger.info(f" * {len(precounted_views)} shouts with views was loaded.")

            # get shout_id by slug
            with local_session() as session:
                for slug, views_count in self.views_by_shout_slug.items():
                    shout_id = session.query(Shout.id).filter(Shout.slug == slug).scalar()
                    if isinstance(shout_id, int):
                        self.views_by_shout_id.update({shout_id: views_count})

        except Exception as e:
            logger.error(f"precounted views loading error: {e}")

    # noinspection PyTypeChecker
    @staticmethod
    async def update_pages():
        """Запрос всех страниц от Google Analytics, отсортированных по количеству просмотров"""
        self = ViewedStorage
        logger.info(" ⎧ views update from Google Analytics ---")
        if not self.disabled:
            try:
                start = time.time()
                async with self.lock:
                    if self.analytics_client:
                        request = RunReportRequest(
                            property=f"properties/{GOOGLE_PROPERTY_ID}",
                            dimensions=[Dimension(name="pagePath")],
                            metrics=[Metric(name="screenPageViews")],
                            date_ranges=[DateRange(start_date=self.start_date, end_date="today")],
                        )
                        response = self.analytics_client.run_report(request)
                        if response and isinstance(response.rows, list):
                            slugs = set()
                            for row in response.rows:
                                print(
                                    row.dimension_values[0].value,
                                    row.metric_values[0].value,
                                )
                                # Извлечение путей страниц из ответа Google Analytics
                                if isinstance(row.dimension_values, list):
                                    page_path = row.dimension_values[0].value
                                    slug = page_path.split("discours.io/")[-1]
                                    views_count = int(row.metric_values[0].value)

                                    # Обновление данных в хранилище
                                    self.views_by_shout[slug] = self.views_by_shout.get(slug, 0)
                                    self.views_by_shout[slug] += views_count
                                    self.update_topics(slug)

                                    # Запись путей страниц для логирования
                                    slugs.add(slug)

                                logger.info(f" ⎪ collected pages: {len(slugs)} ")

                        end = time.time()
                        logger.info(" ⎪ views update time: %fs " % (end - start))
            except Exception as error:
                logger.error(error)
                self.disabled = True

    @staticmethod
    def get_shout(shout_slug="", shout_id=0) -> int:
        """Получение метрики просмотров shout по slug или id."""
        self = ViewedStorage
        return self.views_by_shout_slug.get(shout_slug, self.views_by_shout_id.get(shout_id, 0))

    @staticmethod
    def get_shout_media(shout_slug) -> Dict[str, int]:
        """Получение метрики воспроизведения shout по slug."""
        self = ViewedStorage
        return self.views_by_shout.get(shout_slug, 0)

    @staticmethod
    def get_topic(topic_slug) -> int:
        """Получение суммарного значения просмотров темы."""
        self = ViewedStorage
        return sum(self.views_by_shout.get(shout_slug, 0) for shout_slug in self.shouts_by_topic.get(topic_slug, []))

    @staticmethod
    def get_author(author_slug) -> int:
        """Получение суммарного значения просмотров автора."""
        self = ViewedStorage
        return sum(self.views_by_shout.get(shout_slug, 0) for shout_slug in self.shouts_by_author.get(author_slug, []))

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
            except Exception as exc:
                failed += 1
                logger.debug(exc)
                logger.info(" - update failed #%d, wait 10 secs" % failed)
                if failed > 3:
                    logger.info(" - views update failed, not trying anymore")
                    break
            if failed == 0:
                when = datetime.now(timezone.utc) + timedelta(seconds=self.period)
                t = format(when.astimezone().isoformat())
                logger.info("       ⎩ next update: %s" % (t.split("T")[0] + " " + t.split("T")[1].split(".")[0]))
                await asyncio.sleep(self.period)
            else:
                await asyncio.sleep(10)
                logger.info(" - try to update views again")
