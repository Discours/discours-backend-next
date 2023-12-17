from services.rediscache import redis


async def get_unread_counter(chat_id: str, author_id: int) -> int:
    unread: int = await redis.execute("LLEN", f"chats/{chat_id}/unread/{author_id}") or 0
    return unread


async def get_total_unread_counter(author_id: int) -> int:
    chats_set = await redis.execute("SMEMBERS", f"chats_by_author/{author_id}")
    unread = 0
    if chats_set:
        for chat_id in list(chats_set):
            n = await get_unread_counter(chat_id, author_id)
            unread += n
    return unread
