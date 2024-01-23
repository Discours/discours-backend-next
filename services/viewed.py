import asyncio
import os
from typing import Dict
import logging
import time
import json
from datetime import datetime, timedelta, timezone
from os import environ
# ga
from apiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("\t[services.viewed]\t")
logger.setLevel(logging.DEBUG)

GOOGLE_KEYFILE_PATH = os.environ.get("GOOGLE_KEYFILE_PATH", '/dump/google-service.json')

# Build Analytics Reporting API V4 service object.
def get_service():
    SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
    credentials = Credentials.from_service_account_file(
        GOOGLE_KEYFILE_PATH, scopes=SCOPES
    )
    service = build(serviceName='analyticsreporting', version='v4', credentials=credentials)
    return service


class ViewedStorage:
    lock = asyncio.Lock()
    views_by_shout = {}
    shouts_by_topic = {}
    shouts_by_author = {}
    views = None
    pages = None
    facts = None
    period = 60 * 60  # every hour
    analytics_client = None
    auth_result = None
    disabled = False

    @staticmethod
    async def init():
        """Google Analytics client connection using authentication"""
        self = ViewedStorage
        async with self.lock:
            if os.path.exists(GOOGLE_KEYFILE_PATH):
                self.analytics_client = get_service()
                logger.info(" * authorized permanently by Google Analytics")

                # Load pre-counted views from the JSON file
                self.load_precounted_views()

                views_stat_task = asyncio.create_task(self.worker())
                logger.info(views_stat_task)
            else:
                logger.info(" * please add Google Analytics keyfile")
                self.disabled = True

    @staticmethod
    def load_precounted_views():
        self = ViewedStorage
        try:
            with open("/dump/views.json", "r") as file:
                precounted_views = json.load(file)
                self.views_by_shout.update(precounted_views)
                logger.info(f" * {len(precounted_views)} pre-counted shouts' views loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading pre-counted views: {e}")

    @staticmethod
    async def update_pages():
        """query all the pages from ackee sorted by views count"""
        logger.info(" ⎧ updating ackee pages data ---")
        try:
            start = time.time()
            self = ViewedStorage
            async with self.lock:
                if self.client:
                    # Use asyncio.run to execute asynchronous code in the main entry point
                    self.pages = await asyncio.to_thread(self.client.execute, load_pages)
                    domains = self.pages.get("domains", [])
                    # logger.debug(f" | domains: {domains}")
                    for domain in domains:
                        pages = domain.get("statistics", {}).get("pages", [])
                        if pages:
                            # logger.debug(f" | pages: {pages}")
                            shouts = {}
                            for page in pages:
                                p = page["value"].split("?")[0]
                                slug = p.split("discours.io/")[-1]
                                shouts[slug] = page["count"]
                            for slug in shouts.keys():
                                self.views_by_shout[slug] = self.views_by_shout.get(slug, 0) + 1
                                self.update_topics(slug)
                            logger.info(" ⎪ %d pages collected " % len(shouts.keys()))

                end = time.time()
                logger.info(" ⎪ update_pages took %fs " % (end - start))

        except Exception:
            import traceback

            traceback.print_exc()

    @staticmethod
    async def get_facts():
        self = ViewedStorage
        self.facts = []
        try:
            if self.client:
                async with self.lock:
                    self.facts = await asyncio.to_thread(self.client.execute, load_pages)
        except Exception as er:
            logger.error(f" - get_facts error: {er}")
        return self.facts or []

    @staticmethod
    async def get_shout(shout_slug) -> int:
        """getting shout views metric by slug"""
        self = ViewedStorage
        async with self.lock:
            return self.views_by_shout.get(shout_slug, 0)

    @staticmethod
    async def get_shout_media(shout_slug) -> Dict[str, int]:
        """getting shout plays metric by slug"""
        self = ViewedStorage
        async with self.lock:
            return self.views_by_shout.get(shout_slug, 0)

    @staticmethod
    async def get_topic(topic_slug) -> int:
        """getting topic views value summed"""
        self = ViewedStorage
        topic_views = 0
        async with self.lock:
            for shout_slug in self.shouts_by_topic.get(topic_slug, []):
                topic_views += self.views_by_shout.get(shout_slug, 0)
        return topic_views

    @staticmethod
    async def get_author(author_slug) -> int:
        """getting author views value summed"""
        self = ViewedStorage
        author_views = 0
        async with self.lock:
            for shout_slug in self.shouts_by_author.get(author_slug, []):
                author_views += self.views_by_shout.get(shout_slug, 0)
        return author_views

    @staticmethod
    def update_topics(shout_slug):
        """Updates topics counters by shout slug"""
        self = ViewedStorage
        with local_session() as session:
            # Define a helper function to avoid code repetition
            def update_groups(dictionary, key, value):
                dictionary[key] = list(set(dictionary.get(key, []) + [value]))

            # Update topics and authors using the helper function
            for [_shout_topic, topic] in session.query(ShoutTopic, Topic).join(Topic).join(Shout).where(Shout.slug == shout_slug).all():
                update_groups(self.shouts_by_topic, topic.slug, shout_slug)

            for [_shout_topic, author] in session.query(ShoutAuthor, Author).join(Author).join(Shout).where(Shout.slug == shout_slug).all():
                update_groups(self.shouts_by_author, author.slug, shout_slug)

    @staticmethod
    async def increment(shout_slug):
        """the proper way to change counter"""
        resource = ackee_site + shout_slug
        self = ViewedStorage
        async with self.lock:
            self.views_by_shout[shout_slug] = self.views_by_shout.get(shout_slug, 0) + 1
            self.update_topics(shout_slug)
            variables = {"domainId": domain_id, "input": {"siteLocation": resource}}
            if self.client:
                try:
                    await asyncio.to_thread(self.client.execute, create_record_mutation, variables)
                except Exception as e:
                    logger.error(f"Error during threaded execution: {e}")

    @staticmethod
    async def worker():
        """async task worker"""
        failed = 0
        self = ViewedStorage
        if self.disabled:
            return

        while True:
            try:
                logger.info(" - updating records...")
                await self.update_pages()
                failed = 0
            except Exception:
                failed += 1
                logger.info(" - update failed #%d, wait 10 seconds" % failed)
                if failed > 3:
                    logger.info(" - not trying to update anymore")
                    break
            if failed == 0:
                when = datetime.now(timezone.utc) + timedelta(seconds=self.period)
                t = format(when.astimezone().isoformat())
                logger.info(" ⎩ next update: %s" % (t.split("T")[0] + " " + t.split("T")[1].split(".")[0]))
                await asyncio.sleep(self.period)
            else:
                await asyncio.sleep(10)
                logger.info(" - trying to update data again")
