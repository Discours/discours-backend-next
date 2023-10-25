import json
from services.rediscache import redis


async def notify_reaction(reaction, action: str = "create"):
    channel_name = "reaction"
    data = {
        "payload": reaction, 
        "action": action
    }
    try:
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        print(f"Failed to publish to channel {channel_name}: {e}")


async def notify_shout(shout, action: str = "create"):
    channel_name = "shout"
    data = {
        "payload": shout,
        "action": action
    }
    try:
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        print(f"Failed to publish to channel {channel_name}: {e}")


async def notify_follower(follower: dict, author_id: int, action: str = "follow"):
    fields = follower.keys()
    for k in fields:
        if k not in ["id", "name", "slug", "userpic"]:
            del follower[k]
    channel_name = f"follower:{author_id}"
    data = {
        "payload": follower,
        "action": action
    }
    try:
        await redis.publish(channel_name, json.dumps(data))
    except Exception as e:
        print(f"Failed to publish to channel {channel_name}: {e}")
