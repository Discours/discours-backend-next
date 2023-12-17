import os
from importlib import import_module
from os.path import exists
from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from sentry_sdk.integrations.ariadne import AriadneIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from starlette.applications import Starlette
from starlette.routing import Route

from resolvers.webhook import WebhookEndpoint
from services.rediscache import redis
from services.schema import resolvers
from settings import DEV_SERVER_PID_FILE_NAME, SENTRY_DSN, MODE

import_module("resolvers")
schema = make_executable_schema(load_schema_from_path("schemas/core.graphql"), resolvers)  # type: ignore


async def start_up():
    if MODE == "development":
        if exists(DEV_SERVER_PID_FILE_NAME):
            await redis.connect()
            return
        else:
            with open(DEV_SERVER_PID_FILE_NAME, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
    else:
        await redis.connect()
    try:
        import sentry_sdk

        sentry_sdk.init(
            SENTRY_DSN,
            enable_tracing=True,
            integrations=[
                StarletteIntegration(),
                AriadneIntegration(),
                SqlalchemyIntegration(),
                RedisIntegration(),
                AioHttpIntegration(),
            ],
        )

    except Exception as e:
        print("[sentry] init error")
        print(e)


async def shutdown():
    await redis.disconnect()


routes = [Route("/", GraphQL(schema, debug=True)), Route("/new-author", WebhookEndpoint)]
app = Starlette(routes=routes, debug=True, on_startup=[start_up], on_shutdown=[shutdown])
