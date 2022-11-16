import json
from datetime import datetime, timedelta

from auth.authenticate import login_required
from base.redis import redis
from base.resolvers import query


async def get_unread_counter(chat_id: str, user_slug: str):
    try:
        unread = await redis.execute("LLEN", f"chats/{chat_id}/unread/{user_slug}")
        if unread:
            return unread
    except Exception:
        return 0


async def get_total_unread_counter(user_slug: str):
    chats = await redis.execute("GET", f"chats_by_user/{user_slug}")
    unread = 0
    if chats:
        chats = json.loads(chats)
        for chat_id in chats:
            n = await get_unread_counter(chat_id, user_slug)
            unread += n
    return unread


async def load_messages(chatId: str, offset: int, amount: int):
    ''' load :amount messages for :chatId with :offset '''
    messages = []
    message_ids = await redis.lrange(
        f"chats/{chatId}/message_ids", 0 - offset - amount, 0 - offset
    )
    if message_ids:
        message_keys = [
            f"chats/{chatId}/messages/{mid}" for mid in message_ids
        ]
        messages = await redis.mget(*message_keys)
        messages = [json.loads(msg) for msg in messages]
    return {
        "messages": messages,
        "error": None
    }


@query.field("loadChats")
@login_required
async def load_chats(_, info, offset: int, amount: int):
    """ load :amount chats of current user with :offset """
    user = info.context["request"].user
    chats = await redis.execute("GET", f"chats_by_user/{user.slug}")
    if chats:
        chats = list(json.loads(chats))[offset:offset + amount]
    if not chats:
        chats = []
    for c in chats:
        c['messages'] = await load_messages(c['id'], offset, amount)
        c['unread'] = await get_unread_counter(c['id'], user.slug)
    return {
        "chats": chats,
        "error": None
    }


@query.field("loadMessagesBy")
@login_required
async def load_messages_by(_, info, by, offset: int = 0, amount: int = 50):
    ''' load :amount messages of :chat_id with :offset '''
    user = info.context["request"].user
    my_chats = await redis.execute("GET", f"chats_by_user/{user.slug}")
    chat_id = by.get('chat')
    if chat_id:
        chat = await redis.execute("GET", f"chats/{chat_id}")
        if not chat:
            return {
                "error": "chat not exist"
            }
        messages = await load_messages(chat_id, offset, amount)
    user_id = by.get('author')
    if user_id:
        chats = await redis.execute("GET", f"chats_by_user/{user_id}")
        our_chats = list(set(chats) & set(my_chats))
        for c in our_chats:
            messages += await load_messages(c, offset, amount)
    body_like = by.get('body')
    if body_like:
        for c in my_chats:
            mmm = await load_messages(c, offset, amount)
            for m in mmm:
                if body_like in m["body"]:
                    messages.append(m)
    days = by.get("days")
    if days:
        messages = filter(
            lambda m: datetime.now() - int(m["createdAt"]) < timedelta(days=by.get("days")),
            messages
        )
    return {
        "messages": messages,
        "error": None
    }
