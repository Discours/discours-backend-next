import json

from services.rediscache import redis


async def get_unread_counter(chat_id: str, author_id: int) -> int:
    r = await redis.execute("LLEN", f"chats/{chat_id}/unread/{author_id}")
    if isinstance(r, str):
        return int(r)
    elif isinstance(r, int):
        return r
    else:
        return 0


async def get_total_unread_counter(author_id: int) -> int:
    chats_set = await redis.execute("SMEMBERS", f"chats_by_author/{author_id}")
    s = 0
    if isinstance(chats_set, str):
        chats_set = json.loads(chats_set)
    if isinstance(chats_set, list):
        for chat_id in chats_set:
            s += await get_unread_counter(chat_id, author_id)
    return s
