import asyncio
import time
from datetime import timedelta, timezone, datetime
from os import environ

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

from services.db import local_session
from orm.topic import Topic
from orm.shout import ShoutTopic, Shout

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
    client = None
    auth_result = None
    disabled = False

    @staticmethod
    async def init():
        """graphql client connection using permanent token"""
        self = ViewedStorage
        async with self.lock:
            if token:
                self.client = create_client({"Authorization": "Bearer %s" % str(token)}, schema=schema_str)
                print("[services.viewed] * authorized permanently by ackee.discours.io: %s" % token)
            else:
                print("[services.viewed] * please set ACKEE_TOKEN")
                self.disabled = True

    @staticmethod
    async def update_pages():
        """query all the pages from ackee sorted by views count"""
        print("[services.viewed] ⎧ updating ackee pages data ---")
        start = time.time()
        self = ViewedStorage
        try:
            async with self.client as session:
                self.pages = await session.execute(load_pages)
                self.pages = self.pages["domains"][0]["statistics"]["pages"]
                shouts = {}
                try:
                    for page in self.pages:
                        p = page["value"].split("?")[0]
                        slug = p.split("discours.io/")[-1]
                        shouts[slug] = page["count"]
                    for slug in shouts.keys():
                        await ViewedStorage.increment(slug, shouts[slug])
                except Exception:
                    pass
                print("[services.viewed] ⎪ %d pages collected " % len(shouts.keys()))
        except Exception as e:
            raise e

        end = time.time()
        print("[services.viewed] ⎪ update_pages took %fs " % (end - start))

    @staticmethod
    async def get_facts():
        self = ViewedStorage
        async with self.lock:
            return await self.client.execute(load_facts)

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
            for [shout_topic, topic] in (
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
                print("[services.viewed] - updating views...")
                await self.update_pages()
                failed = 0
            except Exception:
                failed += 1
                print("[services.viewed] - update failed #%d, wait 10 seconds" % failed)
                if failed > 3:
                    print("[services.viewed] - not trying to update anymore")
                    break
            if failed == 0:
                when = datetime.now(timezone.utc) + timedelta(seconds=self.period)
                t = format(when.astimezone().isoformat())
                print("[services.viewed] ⎩ next update: %s" % (t.split("T")[0] + " " + t.split("T")[1].split(".")[0]))
                await asyncio.sleep(self.period)
            else:
                await asyncio.sleep(10)
                print("[services.viewed] - trying to update data again")
