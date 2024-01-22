import asyncio


class FollowingResult:
    def __init__(self, event, kind, payload):
        self.event = event
        self.kind = kind
        self.payload = payload


class Following:
    queue = asyncio.Queue()

    def __init__(self, kind, uid):
        self.kind = kind  # author topic shout community
        self.uid = uid


class FollowingManager:
    lock = asyncio.Lock()
    followers_by_kind = {}
    data = {"author": [], "topic": [], "shout": [], "community": []}

    @staticmethod
    async def register(kind, uid):
        async with FollowingManager.lock:
            FollowingManager.followers_by_kind[kind] = FollowingManager.followers_by_kind.get(kind, [])
            FollowingManager.followers_by_kind[kind].append(uid)

    @staticmethod
    async def remove(kind, uid):
        async with FollowingManager.lock:
            followings = FollowingManager.followers_by_kind.get(kind)
            if followings:
                followings.remove(uid)
                FollowingManager.followers_by_kind[kind] = followings

    @staticmethod
    async def push(kind, payload):
        try:
            async with FollowingManager.lock:
                for entity in FollowingManager.followers_by_kind.get(kind, []):
                    if payload.shout["created_by"] == entity.uid:
                        entity.queue.put_nowait(payload)
        except Exception as e:
            print(Exception(e))
