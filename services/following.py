import asyncio
import logging
import time

from sqlalchemy import and_, joinedload

from orm.author import Author, AuthorFollower
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
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
    followers_by_kind = None
    authors_by_follower = None
    topics_by_follower = None
    shouts_by_follower = None
    authors_by_id = None
    shouts_by_id = None
    topics_by_id = None

    @staticmethod
    async def preload():
        ts = time.time()
        async with FollowingManager.lock:
            followers_by_kind = {'author': {}, 'topic': {}, 'shout': {}}
            authors_by_follower = {}
            topics_by_follower = {}
            shouts_by_follower = {}
            authors_by_id = {}
            topics_by_id = {}
            shouts_by_id = {}
            with local_session() as session:
                all_authors = session.query(Author).all()
                for author in all_authors:
                    authors_by_id[author.id] = author
                all_topics = session.query(Topic).all()
                for topic in all_topics:
                    topics_by_id[topic.id] = topic
                all_shouts = session.query(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None))).all()
                for shout in all_shouts:
                    shouts_by_id[shout.id] = shout

                for kind in followers_by_kind.keys():
                    model_class = MODEL_CLASSES[kind]
                    followings = (
                        session.query(model_class.follower)
                        .distinct()
                        .options(joinedload(model_class.follower))
                        .all()
                    )
                    for following in followings:
                        if kind == 'topic':
                            followers_by_kind[kind][following.topic] = followers_by_kind[kind].get(following.topic, set())
                            followers_by_kind[kind][following.topic].add(following.follower)
                        elif kind == 'author':
                            followers_by_kind[kind][following.author] = followers_by_kind[kind].get(following.author, set())
                            followers_by_kind[kind][following.author].add(following.follower)
                        elif kind == 'shout':
                            followers_by_kind[kind][following.shout] = followers_by_kind[kind].get(following.shout, set())
                            followers_by_kind[kind][following.shout].add(following.follower)

                # Load authors_by_follower, topics_by_follower, and shouts_by_follower
                for entity_kind in followers_by_kind.keys():
                    followers_dict = followers_by_kind[entity_kind]
                    if followers_dict:
                        entity_class = MODEL_CLASSES[entity_kind]
                        followings = (
                            session.query(entity_class)
                            .options(joinedload(entity_class.follower), joinedload(entity_class.entity))
                            .all()
                        )
                        for following in followings:
                            follower_id = following.follower.id
                            entity_id = following.entity.id
                            followers_dict.setdefault(follower_id, set()).add(entity_id)

                # Assign the loaded dictionaries to the class attributes
                FollowingManager.authors_by_follower = authors_by_follower
                FollowingManager.topics_by_follower = topics_by_follower
                FollowingManager.shouts_by_follower = shouts_by_follower
                FollowingManager.authors_by_id = authors_by_id
                FollowingManager.topics_by_id = topics_by_id
                FollowingManager.shouts_by_id = shouts_by_id

        logger.info(f' preloaded in {time.time() - ts} msec')

    @staticmethod
    async def register(entity_kind, entity_id, follower_id):
        self = FollowingManager
        try:
            async with self.lock:
                if isinstance(self.authors_by_id, dict):
                    follower = self.authors_by_id.get(follower_id)
                    if follower and self.followers_by_kind:
                        self.followers_by_kind[entity_kind][entity_id] = self.followers_by_kind[entity_kind].get(entity_id, set())
                        self.followers_by_kind[entity_kind][entity_id].add(follower)
                        if entity_kind == 'author' and self.authors_by_follower and self.authors_by_id:
                            author = self.authors_by_id.get(entity_id)
                            self.authors_by_follower.setdefault(follower_id, set()).add(author)
                        if entity_kind == 'topic' and self.topics_by_follower and self.topics_by_id:
                            topic = self.topics_by_id.get(entity_id)
                            self.topics_by_follower.setdefault(follower_id, set()).add(topic)
                        if entity_kind == 'shout' and self.shouts_by_follower and self.shouts_by_id:
                            shout = self.shouts_by_id.get(entity_id)
                            self.shouts_by_follower.setdefault(follower_id, set()).add(shout)
        except Exception as exc:
            logger.warn(exc)

    @staticmethod
    async def remove(entity_kind, entity_id, follower_id):
        self = FollowingManager
        async with self.lock:
            if self.followers_by_kind and entity_kind in self.followers_by_kind and entity_id in self.followers_by_kind[entity_kind]:
                try:
                    del self.followers_by_kind[entity_kind][entity_id]
                    if entity_kind == 'author' and self.authors_by_follower:
                        del self.authors_by_follower[follower_id][entity_id]
                    elif entity_kind == 'topic' and self.topics_by_follower:
                        del self.topics_by_follower[follower_id][entity_id]
                    elif entity_kind == 'shout' and self.shouts_by_follower:
                        del self.shouts_by_follower[follower_id][entity_id]
                except Exception as exc:
                    logger.warn(exc)
                if isinstance(self.authors_by_id, dict):
                    follower = self.authors_by_id.get(follower_id)
                    if follower:
                        self.followers_by_kind[entity_kind][entity_id].remove(follower)

    @staticmethod
    async def get_followers_by_kind(kind, target_id=None):
        async with FollowingManager.lock:
            if FollowingManager.followers_by_kind:
                return (
                    FollowingManager.followers_by_kind[kind] if target_id is None else {target_id}
                )

    @staticmethod
    async def get_authors_for(follower_id):
        async with FollowingManager.lock:
            if FollowingManager.authors_by_follower:
                return FollowingManager.authors_by_follower.get(follower_id, set())

    @staticmethod
    async def get_topics_for(follower_id):
        async with FollowingManager.lock:
            if FollowingManager.topics_by_follower:
                return FollowingManager.topics_by_follower.get(follower_id, set())

    @staticmethod
    async def get_shouts_for(follower_id):
        async with FollowingManager.lock:
            if FollowingManager.shouts_by_follower:
                return FollowingManager.shouts_by_follower.get(follower_id, set())
