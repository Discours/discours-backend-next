import asyncio
from logging import Logger
import time
from datetime import datetime, timedelta, timezone
from os import environ
import logging
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from orm.shout import Shout, ShoutTopic
from orm.topic import Topic
from services.db import local_session


logging.basicConfig()
logger = logging.getLogger("\t[services.viewed]\t")
logger.setLevel(logging.DEBUG)


load_facts = gql(
    """ query getDomains {
    domains {
        id
        title
        facts {
            activeVisitors
            viewsToday
            viewsMonth
            viewsYear
        }
    } } """
)

load_pages = gql(
    """ query getDomains {
    domains {
    title
    statistics {
        pages(sorting: TOP) {
                # id
                count
                # created
                value
            }
        }
    } } """
)

schema_str = open("schemas/ackee.graphql").read()
token = environ.get("ACKEE_TOKEN", "")


def create_client(headers=None, schema=None):
    transport = AIOHTTPTransport(
        url="https://ackee.discours.io/api",
        headers=headers,
    )
    return Client(schema=schema, transport=transport)


class ViewedStorage:
    lock = asyncio.Lock()
    by_shouts = {}
    by_topics = {}
    by_reactions = {}
    views = None
    pages = None
    domains = None
    period = 60 * 60  # every hour
    client: Client | None = None
    auth_result = None
    disabled = False

    @staticmethod
    async def init():
        """graphql client connection using permanent token"""
        self = ViewedStorage
        async with self.lock:
            if token:
                self.client = create_client({"Authorization": f"Bearer {token}"}, schema=schema_str)
                logger.info(" * authorized permanently by ackee.discours.io: %s" % token)

                views_stat_task = asyncio.create_task(self.worker())
                logger.info(views_stat_task)
            else:
                logger.info(" * please set ACKEE_TOKEN")
                self.disabled = True

    @staticmethod
    async def update_pages():
        """query all the pages from ackee sorted by views count"""
        logger.info(" ⎧ updating ackee pages data ---")
        try:
            start = time.time()
            self = ViewedStorage
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
                            await ViewedStorage.increment(slug, shouts[slug])
                        logger.info(" ⎪ %d pages collected " % len(shouts.keys()))

                end = time.time()
                logger.info(" ⎪ update_pages took %fs " % (end - start))

        except Exception:
            import traceback
            traceback.print_exc()

    @staticmethod
    async def get_facts():
        self = ViewedStorage
        facts = []
        try:
            if self.client:
                async with self.lock:
                    facts = await self.client.execute(load_facts)
        except Exception as er:
            logger.error(f" - get_facts error: {er}")
        return facts or []

    @staticmethod
    async def get_shout(shout_slug):
        """getting shout views metric by slug"""
        self = ViewedStorage
        async with self.lock:
            return self.by_shouts.get(shout_slug, 0)

    @staticmethod
    async def get_reaction(shout_slug, reaction_id):
        """getting reaction views metric by slug"""
        self = ViewedStorage
        async with self.lock:
            return self.by_reactions.get(shout_slug, {}).get(reaction_id, 0)

    @staticmethod
    async def get_topic(topic_slug):
        """getting topic views value summed"""
        self = ViewedStorage
        topic_views = 0
        async with self.lock:
            for shout_slug in self.by_topics.get(topic_slug, {}).keys():
                topic_views += self.by_topics[topic_slug].get(shout_slug, 0)
        return topic_views

    @staticmethod
    def update_topics(shout_slug):
        """updates topics counters by shout slug"""
        self = ViewedStorage
        with local_session() as session:
            for [_shout_topic, topic] in (
                session.query(ShoutTopic, Topic).join(Topic).join(Shout).where(Shout.slug == shout_slug).all()
            ):
                if not self.by_topics.get(topic.slug):
                    self.by_topics[topic.slug] = {}
                self.by_topics[topic.slug][shout_slug] = self.by_shouts[shout_slug]

    @staticmethod
    async def increment(shout_slug, amount=1, viewer="ackee"):
        """the only way to change views counter"""
        self = ViewedStorage
        async with self.lock:
            self.by_shouts[shout_slug] = self.by_shouts.get(shout_slug, 0) + amount
            self.update_topics(shout_slug)

    @staticmethod
    async def increment_reaction(shout_slug, reaction_id, amount=1, viewer="ackee"):
        """the only way to change views counter"""
        self = ViewedStorage
        async with self.lock:
            self.by_reactions[shout_slug][reaction_id] = self.by_reactions[shout_slug].get(reaction_id, 0) + amount
            self.update_topics(shout_slug)

    @staticmethod
    async def worker():
        """async task worker"""
        failed = 0
        self = ViewedStorage
        if self.disabled:
            return

        while True:
            try:
                logger.info(" - updating views...")
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
