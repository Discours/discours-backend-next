import asyncio
import logging
import time

from orm.author import AuthorFollower
from orm.shout import ShoutReactionsFollower
from orm.topic import TopicFollower
from services.db import local_session


logger = logging.getLogger('[services.following] ')
logger.setLevel(logging.DEBUG)


MODEL_CLASSES = {'author': AuthorFollower, 'topic': TopicFollower, 'shout': ShoutReactionsFollower}


class FollowingResult:
    def __init__(self, event, kind, payload):
        self.event = event
        self.kind = kind
        self.payload = payload


class Following:
    def __init__(self, kind, uid):
        self.kind = kind  # author, topic, shout
        self.uid = uid
        self.queue = asyncio.Queue()


class FollowingManager:
    lock = asyncio.Lock()
    followers_by_kind = {'author': [], 'topic': [], 'shout': []}
    authors_by_follower = {}
    topics_by_follower = {}
    shouts_by_follower = {}

    @staticmethod
    async def preload():
        logger.info(' preloading started...')
        ts = int(time.time())
        async with FollowingManager.lock:
            with local_session() as session:
                # Load followers_by_kind
                for kind in FollowingManager.followers_by_kind.keys():
                    model_class = MODEL_CLASSES[kind]
                    followers = session.query(model_class.follower).distinct().all()
                    FollowingManager.followers_by_kind[kind] = [follower[0] for follower in followers]

                # Load authors_by_follower
                for following in session.query(AuthorFollower).all():
                    FollowingManager.authors_by_follower[following.follower] = FollowingManager.authors_by_follower.get(
                        following.follower, []
                    )
                    FollowingManager.authors_by_follower[following.follower].append(following.author)

                # Load topics_by_follower
                for following in session.query(TopicFollower).all():
                    FollowingManager.topics_by_follower[following.follower] = FollowingManager.topics_by_follower.get(
                        following.follower, []
                    )
                    FollowingManager.topics_by_follower[following.follower].append(following.topic)

                # Load shouts_by_follower
                for following in session.query(ShoutReactionsFollower).all():
                    FollowingManager.shouts_by_follower[following.follower] = FollowingManager.shouts_by_follower.get(
                        following.follower, []
                    )
                    FollowingManager.shouts_by_follower[following.follower].append(following.shout)
        logger.info(f' preloading finished at {(int(time.time()) - ts)/1000} secs')

    @staticmethod
    async def register(kind, uid):
        async with FollowingManager.lock:
            if uid not in FollowingManager.followers_by_kind[kind]:
                FollowingManager.followers_by_kind[kind].append(uid)

    @staticmethod
    async def remove(kind, uid):
        async with FollowingManager.lock:
            FollowingManager.followers_by_kind[kind] = [
                follower for follower in FollowingManager.followers_by_kind[kind] if follower != uid
            ]

    @staticmethod
    async def push(kind, payload):
        try:
            async with FollowingManager.lock:
                for entity in FollowingManager.followers_by_kind[kind]:
                    if payload.shout['created_by'] == entity:
                        await entity.queue.put(payload)
        except Exception as e:
            print(f'Error in push method: {e}')

    @staticmethod
    async def get_followers_by_kind(kind, target_id=None):
        async with FollowingManager.lock:
            return (
                FollowingManager.followers_by_kind[kind][target_id]
                if target_id
                else FollowingManager.followers_by_kind[kind]
            )

    @staticmethod
    async def get_authors_for(follower_id):
        async with FollowingManager.lock:
            return FollowingManager.authors_by_follower.get(follower_id, [])

    @staticmethod
    async def get_topics_for(follower_id):
        async with FollowingManager.lock:
            return FollowingManager.topics_by_follower.get(follower_id, [])

    @staticmethod
    async def get_shouts_for(follower_id):
        async with FollowingManager.lock:
            return FollowingManager.shouts_by_follower.get(follower_id, [])
