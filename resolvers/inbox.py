from orm import Message, User
from orm.base import local_session

from resolvers.base import mutation, query, subscription

from auth.authenticate import login_required

import asyncio, uuid, json
from datetime import datetime

from redis import redis

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
	async def put(msg):
		async with MessageSubscriptions.lock:
			for subs in MessageSubscriptions.subscriptions:
				subs.put_nowait(msg)

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

	return { "chatId" : chat_id }

@query.field("enterChat")
@login_required
async def enter_chat(_, info, chatId):
	chat = await redis.execute("GET", f"chats/{chatId}")
	if not chat:
		return { "error" : "chat not exist" }
	chat = json.loads(chat)

	messages = await redis.lrange(f"chats/{chatId}/messages", 0, 10)
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

	new_message = {
		"chatId" : chatId,
		"author" : user.slug,
		"body" : body,
		"replyTo" : replyTo
	}

	message_id = await redis.execute("LPUSH", f"chats/{chatId}/messages", json.dumps(new_message))
	new_message["id"] = message_id

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
async def update_message(_, info, id, body):
	auth = info.context["request"].auth
	user_id = auth.user_id
	
	with local_session() as session:
		try:
			message = check_and_get_message(id, user_id, session)
		except Exception as err:
			return {"error" : err}
	
		message.body = body
		session.commit()
	
	result = MessageResult("UPDATED", message)
	await MessageSubscriptions.put(result)
	
	return {"message" : message}

@mutation.field("deleteMessage")
@login_required
async def delete_message(_, info, id):
	auth = info.context["request"].auth
	user_id = auth.user_id
	
	with local_session() as session:
		try:
			message = check_and_get_message(id, user_id, session)
		except Exception as err:
			return {"error" : err}
	
		session.delete(message)
		session.commit()
	
	result = MessageResult("DELETED", message)
	await MessageSubscriptions.put(result)
	
	return {}


@subscription.source("messageChanged")
async def new_message_generator(obj, info):
	try:
		msg_queue = asyncio.Queue()
		await MessageSubscriptions.register_subscription(msg_queue)
		while True:
			msg = await msg_queue.get()
			yield msg
	finally:
		await MessageSubscriptions.del_subscription(msg_queue)

@subscription.field("messageChanged")
def message_resolver(message, info):
	return message
