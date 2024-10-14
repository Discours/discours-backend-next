import asyncio
import os
from importlib import import_module
from os.path import exists

from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from cache.precache import precache_data
from cache.revalidator import revalidation_manager
from orm import (  # , collection, invite
    author,
    community,
    notification,
    reaction,
    shout,
    topic,
)
from services.db import create_table_if_not_exists, engine
from services.exception import ExceptionHandlerMiddleware
from services.redis import redis
from services.schema import resolvers
from services.search import search_service
from services.viewed import ViewedStorage
from services.webhook import WebhookEndpoint
from settings import DEV_SERVER_PID_FILE_NAME, MODE

import_module("resolvers")
schema = make_executable_schema(load_schema_from_path("schema/"), resolvers)


async def start():
    if MODE == "development":
        if not exists(DEV_SERVER_PID_FILE_NAME):
            # pid file management
            with open(DEV_SERVER_PID_FILE_NAME, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
    print(f"[main] process started in {MODE} mode")


def create_all_tables():
    for model in [
        author.Author,
        author.AuthorRating,
        author.AuthorFollower,
        notification.Notification,
        notification.NotificationSeen,
        shout.Shout,
        shout.ShoutAuthor,
        shout.ShoutTopic,
        shout.ShoutCommunity,
        topic.Topic,
        topic.TopicFollower,
        reaction.Reaction,
        community.Community,
        community.CommunityFollower,
        # collection.Collection, collection.ShoutCollection,
        # invite.Invite
    ]:
        create_table_if_not_exists(engine, model)


async def create_all_tables_async():
    # Оборачиваем синхронную функцию в асинхронную
    await asyncio.to_thread(create_all_tables)


async def lifespan(app):
    # Запуск всех сервисов при старте приложения
    await asyncio.gather(
        create_all_tables_async(),
        redis.connect(),
        precache_data(),
        ViewedStorage.init(),
        search_service.info(),
        start(),
        revalidation_manager.start(),
    )
    yield
    # Остановка сервисов при завершении работы приложения
    await redis.disconnect()


# Создаем экземпляр GraphQL
graphql_app = GraphQL(schema, debug=True)


# Оборачиваем GraphQL-обработчик для лучшей обработки ошибок
async def graphql_handler(request):
    try:
        return await graphql_app.handle_request(request)
    except asyncio.CancelledError:
        return JSONResponse({"error": "Request cancelled"}, status_code=499)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# main starlette app object with ariadne mounted in root
app = Starlette(
    routes=[
        Route("/", graphql_handler),
        Route("/new-author", WebhookEndpoint),
    ],
    lifespan=lifespan,
    debug=True,
)

app.add_middleware(ExceptionHandlerMiddleware)
