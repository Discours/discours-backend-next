import asyncio
import os
from importlib import import_module
from os.path import exists

from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from cache.precache import precache_data
from cache.revalidator import revalidation_manager
from orm import (
    # collection,
    # invite,
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
        # user.User,
        author.Author,
        author.AuthorFollower,
        community.Community,
        community.CommunityFollower,
        shout.Shout,
        shout.ShoutAuthor,
        author.AuthorBookmark,
        topic.Topic,
        topic.TopicFollower,
        shout.ShoutTopic,
        reaction.Reaction,
        shout.ShoutReactionsFollower,
        author.AuthorRating,
        notification.Notification,
        notification.NotificationSeen,
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
async def graphql_handler(request: Request):
    if request.method not in ["GET", "POST"]:
        return JSONResponse({"error": "Method Not Allowed"}, status_code=405)

    try:
        result = await graphql_app.handle_request(request)
        if isinstance(result, Response):
            return result
        return JSONResponse(result)
    except asyncio.CancelledError:
        return JSONResponse({"error": "Request cancelled"}, status_code=499)
    except Exception as e:
        print(f"GraphQL error: {str(e)}")
        return JSONResponse({"error": str(e)}, status_code=500)


# Обновляем маршрут в Starlette
app = Starlette(
    routes=[
        Route("/", graphql_handler, methods=["GET", "POST"]),
        Route("/new-author", WebhookEndpoint),
    ],
    lifespan=lifespan,
    debug=True,
)

app.add_middleware(ExceptionHandlerMiddleware)
