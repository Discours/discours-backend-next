import asyncio
import time
from datetime import timedelta, timezone, datetime
from os import environ

from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport

from services.db import local_session
from orm import Topic
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
    return Client(
        schema=schema,
        transport=HTTPXAsyncTransport(
            url="https://ackee.discours.io/api",
            headers=headers,
        ),
    )


class ViewedStorage:
    lock = asyncio.Lock()
    by_shouts = {}
    by_topics = {}
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
                self.client = create_client(
                    {"Authorization": "Bearer %s" % str(token)}, schema=schema_str
                )
                print(
                    "[stat] * authorized permanentely by ackee.discours.io: %s" % token
                )
            else:
                print("[stat] * please set ACKEE_TOKEN")
                self.disabled = True

    @staticmethod
    async def update_pages():
        """query all the pages from ackee sorted by views count"""
        print("[stat] ⎧ updating ackee pages data ---")
        start = time.time()
        self = ViewedStorage
        try:
            self.pages = await self.client.execute_async(load_pages)
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
            print("[stat] ⎪ %d pages collected " % len(shouts.keys()))
        except Exception as e:
            raise e

        end = time.time()
        print("[stat] ⎪ update_pages took %fs " % (end - start))

    @staticmethod
    async def get_facts():
        self = ViewedStorage
        async with self.lock:
            return self.client.execute_async(load_facts)

    # unused yet
    @staticmethod
    async def get_shout(shout_slug):
        """getting shout views metric by slug"""
        self = ViewedStorage
        async with self.lock:
            shout_views = self.by_shouts.get(shout_slug)
            if not shout_views:
                shout_views = 0
                with local_session() as session:
                    try:
                        shout = (
                            session.query(Shout).where(Shout.slug == shout_slug).one()
                        )
                        self.by_shouts[shout_slug] = shout.views
                        self.update_topics(session, shout_slug)
                    except Exception as e:
                        raise e

            return shout_views

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
    def update_topics(session, shout_slug):
        """updates topics counters by shout slug"""
        self = ViewedStorage
        for [shout_topic, topic] in (
            session.query(ShoutTopic, Topic)
            .join(Topic)
            .join(Shout)
            .where(Shout.slug == shout_slug)
            .all()
        ):
            if not self.by_topics.get(topic.slug):
                self.by_topics[topic.slug] = {}
            self.by_topics[topic.slug][shout_slug] = self.by_shouts[shout_slug]

    @staticmethod
    async def increment(shout_slug, amount=1, viewer="ackee"):
        """the only way to change views counter"""
        self = ViewedStorage
        async with self.lock:
            # TODO optimize, currenty we execute 1 DB transaction per shout
            with local_session() as session:
                shout = session.query(Shout).where(Shout.slug == shout_slug).one()
                if viewer == "old-discours":
                    # this is needed for old db migration
                    if shout.viewsOld == amount:
                        print(f"[stat] ⎪ viewsOld amount: {amount}")
                    else:
                        print(
                            f"[stat] ⎪ viewsOld amount changed: {shout.viewsOld} --> {amount}"
                        )
                        shout.viewsOld = amount
                else:
                    if shout.viewsAckee == amount:
                        print(f"[stat] ⎪ viewsAckee amount: {amount}")
                    else:
                        print(
                            f"[stat] ⎪ viewsAckee amount changed: {shout.viewsAckee} --> {amount}"
                        )
                        shout.viewsAckee = amount

                session.commit()

                # this part is currently unused
                self.by_shouts[shout_slug] = self.by_shouts.get(shout_slug, 0) + amount
                self.update_topics(session, shout_slug)

    @staticmethod
    async def worker():
        """async task worker"""
        failed = 0
        self = ViewedStorage
        if self.disabled:
            return

        while True:
            try:
                print("[stat] - updating views...")
                await self.update_pages()
                failed = 0
            except Exception:
                failed += 1
                print("[stat] - update failed #%d, wait 10 seconds" % failed)
                if failed > 3:
                    print("[stat] - not trying to update anymore")
                    break
            if failed == 0:
                when = datetime.now(timezone.utc) + timedelta(seconds=self.period)
                t = format(when.astimezone().isoformat())
                print(
                    "[stat] ⎩ next update: %s"
                    % (t.split("T")[0] + " " + t.split("T")[1].split(".")[0])
                )
                await asyncio.sleep(self.period)
            else:
                await asyncio.sleep(10)
                print("[stat] - trying to update data again")
