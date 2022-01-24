from orm import User
from orm.base import local_session

from resolvers.base import mutation, query, subscription

from auth.authenticate import login_required

import asyncio, uuid, json
from datetime import datetime

from redis import redis

class MessageSubscription:
	queue = asyncio.Queue()

	def __init__(self, chat_id):
		self.chat_id = chat_id

class MessageSubscriptions:
	lock = asyncio.Lock()
	subscriptions = []

	@staticmethod
	async def register_subscription(subs):
		async with MessageSubscriptions.lock:
			MessageSubscriptions.subscriptions.append(subs)
	
	@staticmethod
	async def del_subscription(subs):
		async with MessageSubscriptions.lock:
			MessageSubscriptions.subscriptions.remove(subs)
	
	@staticmethod
	async def put(message_result):
		async with MessageSubscriptions.lock:
			for subs in MessageSubscriptions.subscriptions:
				if message_result.message["chatId"] == subs.chat_id:
					subs.queue.put_nowait(message_result)

class MessageResult:
	def __init__(self, status, message):
		self.status = status
		self.message = message

@mutation.field("createChat")
@login_required
async def create_chat(_, info, description):
	user = info.context["request"].user

	chat_id = uuid.uuid4()
	chat = {
		"description" : description,
		"createdAt" : str(datetime.now),
		"createdBy" : user.slug,
		"id" : str(chat_id)
	}

	await redis.execute("SET", f"chats/{chat_id}", json.dumps(chat))
	await redis.execute("SET", f"chats/{chat_id}/next_message_id", 0)

	return { "chatId" : chat_id }

@query.field("enterChat")
@login_required
async def enter_chat(_, info, chatId):
	chat = await redis.execute("GET", f"chats/{chatId}")
	if not chat:
		return { "error" : "chat not exist" }
	chat = json.loads(chat)

	message_ids = await redis.lrange(f"chats/{chatId}/message_ids", 0, 10)
	messages = []
	if message_ids:
		message_keys = [f"chats/{chatId}/messages/{id.decode('UTF-8')}" for id in message_ids]
		messages = await redis.mget(*message_keys)
		messages = [json.loads(msg) for msg in messages]

	return { 
		"chat" : chat,
		"messages" : messages 
	}

@mutation.field("createMessage")
@login_required
async def create_message(_, info, chatId, body, replyTo = None):
	user = info.context["request"].user

	chat = await redis.execute("GET", f"chats/{chatId}")
	if not chat:
		return { "error" : "chat not exist" }

	message_id = await redis.execute("GET", f"chats/{chatId}/next_message_id")
	message_id = int(message_id)

	new_message = {
		"chatId" : chatId,
		"id" : message_id,
		"author" : user.slug,
		"body" : body,
		"replyTo" : replyTo
	}

	await redis.execute("SET", f"chats/{chatId}/messages/{message_id}", json.dumps(new_message))
	await redis.execute("LPUSH", f"chats/{chatId}/message_ids", str(message_id))
	await redis.execute("SET", f"chats/{chatId}/next_message_id", str(message_id + 1))

	result = MessageResult("NEW", new_message)
	await MessageSubscriptions.put(result)

	return {"message" : new_message}

@query.field("getMessages")
@login_required
async def get_messages(_, info, count, page):
	auth = info.context["request"].auth
	user_id = auth.user_id
	
	with local_session() as session:
		messages = session.query(Message).filter(Message.author == user_id)
	
	return messages

def check_and_get_message(message_id, user_id, session) :
	message = session.query(Message).filter(Message.id == message_id).first()
	
	if not message :
		raise Exception("invalid id")
	
	if message.author != user_id :
		raise Exception("access denied")
	
	return message

@mutation.field("updateMessage")
@login_required
async def update_message(_, info, chatId, id, body):
	user = info.context["request"].user

	chat = await redis.execute("GET", f"chats/{chatId}")
	if not chat:
		return { "error" : "chat not exist" }

	message = await redis.execute("GET", f"chats/{chatId}/messages/{id}")
	if not message:
		return { "error" : "message  not exist" }

	message = json.loads(message)
	message["body"] = body

	await redis.execute("SET", f"chats/{chatId}/messages/{id}", json.dumps(message))

	result = MessageResult("UPDATED", message)
	await MessageSubscriptions.put(result)

	return {"message" : message}

@mutation.field("deleteMessage")
@login_required
async def delete_message(_, info, chatId, id):
	user = info.context["request"].user

	chat = await redis.execute("GET", f"chats/{chatId}")
	if not chat:
		return { "error" : "chat not exist" }

	message = await redis.execute("GET", f"chats/{chatId}/messages/{id}")
	if not message:
		return { "error" : "message  not exist" }
	message = json.loads(message)

	await redis.execute("LREM", f"chats/{chatId}/message_ids", 0, str(id))
	await redis.execute("DEL", f"chats/{chatId}/messages/{id}")

	result = MessageResult("DELETED", message)
	await MessageSubscriptions.put(result)

	return {}


@subscription.source("chatUpdated")
async def message_generator(obj, info, chatId):
	try:
		subs = MessageSubscription(chatId)
		await MessageSubscriptions.register_subscription(subs)
		while True:
			msg = await subs.queue.get()
			yield msg
	finally:
		await MessageSubscriptions.del_subscription(subs)

@subscription.field("chatUpdated")
def message_resolver(message, info, chatId):
	return message
