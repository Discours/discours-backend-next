import asyncio
import threading
from logging import Logger
import time
from datetime import datetime, timedelta, timezone
from os import environ
import logging
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from graphql import DocumentNode

from orm.shout import Shout, ShoutTopic
from orm.topic import Topic
from services.db import local_session

logging.basicConfig(
    format="[%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    level=logging.DEBUG,
    handlers=[
        logging.StreamHandler(),
    ],
)
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

create_record_mutation_string = """
    createRecord(domainId: $domainId, input: $input) {
        payload {
            id
        }
    }
"""

create_record_mutation = gql(f"mutation {{{create_record_mutation_string}}}")

schema_str = open("schemas/stat.graphql").read()
token = environ.get("ACKEE_TOKEN", "")
domain_id = environ.get("ACKEE_DOMAIN_ID", "")
ackee_site = environ.get("ACKEE_SITE", "https://testing.discours.io/")


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
    by_authors = {}
    views = None
    pages = None
    domains = None
    facts = None
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
                                self.by_shouts[slug] = self.by_shouts.get(slug, 0) + 1
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
            return self.by_shouts.get(shout_slug, 0)

    @staticmethod
    async def get_shout_media(shout_slug) -> Dict[str, int]:
        """getting shout plays metric by slug"""
        self = ViewedStorage
        async with self.lock:
            return self.by_shouts.get(shout_slug, 0)

    @staticmethod
    async def get_topic(topic_slug) -> int:
        """getting topic views value summed"""
        self = ViewedStorage
        topic_views = 0
        async with self.lock:
            for shout_slug in self.by_topics.get(topic_slug, {}).keys():
                topic_views += self.by_topics[topic_slug].get(shout_slug, 0)
        return topic_views

    @staticmethod
    async def get_authors(author_slug) -> int:
        """getting author views value summed"""
        self = ViewedStorage
        author_views = 0
        async with self.lock:
            for shout_slug in self.by_authors.get(author_slug, {}).keys():
                author_views += self.by_authors[author_slug].get(shout_slug, 0)
        return author_views

    @staticmethod
    def update_topics(shout_slug):
        """updates topics counters by shout slug"""
        self = ViewedStorage
        with local_session() as session:
            # grouped by topics
            for [_shout_topic, topic] in (
                session.query(ShoutTopic, Topic).join(Topic).join(Shout).where(Shout.slug == shout_slug).all()
            ):
                if not self.by_topics.get(topic.slug):
                    self.by_topics[topic.slug] = {}
                self.by_topics[topic.slug][shout_slug] = self.by_shouts[shout_slug]

            # grouped by authors
            for [_shout_author, author] in (
                session.query(ShoutAuthor, Author).join(Author).join(Shout).where(Shout.slug == shout_slug).all()
            ):
                if not self.by_authors.get(author.slug):
                    self.by_authors[author.slug] = {}
                self.by_authors[author.slug][shout_slug] = self.by_shouts[shout_slug]

    @staticmethod
    async def increment(shout_slug):
        """the proper way to change counter"""
        resource = ackee_site + shout_slug
        self = ViewedStorage
        async with self.lock:
            self.by_shouts[shout_slug] = self.by_shouts.get(shout_slug, 0) + 1
            self.update_topics(shout_slug)
            variables = {"domainId": domain_id, "input": {"siteLocation": resource}}
            if self.client:
                try:
                    await asyncio.to_thread(self.client.execute, create_record_mutation, variables)
                except Exception as e:
                    logger.error(f"Error during threaded execution: {e}")

    @staticmethod
    async def increment_amount(shout_slug, amount):
        """the migration way to change counter with batching"""
        resource = ackee_site + shout_slug
        self = ViewedStorage

        gql_string = ""
        batch_size = 100
        if not isinstance(amount, int):
            try:
                amount = int(amount)
                if not isinstance(amount, int):
                    amount = 1
            except:
                pass

        self.by_shouts[shout_slug] = self.by_shouts.get(shout_slug, 0) + amount
        self.update_topics(shout_slug)
        logger.info(f"{int(amount/100) + 1} requests")
        for i in range(amount):
            alias = f"mutation{i + 1}"
            gql_string += f"{alias}: {create_record_mutation_string
                .replace('$domainId', f'"{domain_id}"')
                .replace('$input', f'{{siteLocation: "{resource}"}}')
            }\n"
            # Execute the batch every 100 records
            if (i + 1) % batch_size == 0 or (i + 1) == amount:
                await self.exec(f"mutation {{\n{gql_string}\n}}")
                gql_string = ""  # Reset the gql_string for the next batch
                # Throttle the requests to 3 per second
                await asyncio.sleep(1 / 3)


        logger.info(f"Incremented {amount} records for shout_slug: {shout_slug}")


    @staticmethod
    async def exec(gql_string: str):
        self = ViewedStorage
        async with self.lock:
            if self.client:
                try:
                    await asyncio.to_thread(self.client.execute, gql(gql_string))
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
