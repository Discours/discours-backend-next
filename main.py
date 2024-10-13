import os
from importlib import import_module
from os.path import exists

from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from starlette.applications import Starlette
from starlette.routing import Route

from cache.precache import precache_data
from cache.revalidator import revalidation_manager
from services.exception import ExceptionHandlerMiddleware
from services.redis import redis
from services.schema import resolvers
from services.search import search_service
from services.viewed import ViewedStorage
from services.webhook import WebhookEndpoint
from settings import DEV_SERVER_PID_FILE_NAME, MODE
from services.db import engine, create_table_if_not_exists
from orm import author, notification, shout, topic, reaction, community #, collection, invite

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
    for model in [author.Author, author.AuthorRating, author.AuthorFollower,
                  notification.Notification, notification.NotificationSeen,
                  shout.Shout, shout.ShoutAuthor, shout.ShoutTopic, shout.ShoutCommunity,
                  topic.Topic, topic.TopicFollower,
                  reaction.Reaction,
                  community.Community, community.CommunityFollower,
                  # collection.Collection, collection.ShoutCollection,
                  # invite.Invite
                  ]:
        create_table_if_not_exists(engine, model)


# main starlette app object with ariadne mounted in root
app = Starlette(
    routes=[
        Route("/", GraphQL(schema, debug=True)),
        Route("/new-author", WebhookEndpoint),
    ],
    on_startup=[
        create_all_tables,
        redis.connect,
        precache_data,
        ViewedStorage.init,
        search_service.info,
        # start_sentry,
        start,
        revalidation_manager.start,
    ],
    on_shutdown=[redis.disconnect],
    debug=True,
)
app.add_middleware(ExceptionHandlerMiddleware)
