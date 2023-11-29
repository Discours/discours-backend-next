import os
from importlib import import_module
from os.path import exists
from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from sentry_sdk.integrations.ariadne import AriadneIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
from starlette.applications import Starlette
from starlette.endpoints import HTTPEndpoint, Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from resolvers.author import create_author
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
            integrations=[AriadneIntegration(), SqlalchemyIntegration(), RedisIntegration()],
        )

    except Exception as e:
        print("[sentry] init error")
        print(e)


async def shutdown():
    await redis.disconnect()


class WebhookEndpoint(HTTPEndpoint):
    async def post(self, request: Request) -> JSONResponse:
        try:
            data = await request.json()
            if data:
                auth = request.headers.get("Authorization")
                if auth:
                    if auth == os.environ.get("WEBHOOK_SECRET"):
                        # Extract user_id and slug
                        user_id = data["user"]["id"]
                        email_slug = data["user"]["email"].replace(".", "-").split("@").pop()
                        slug = data["user"]["preferred_username"] or email_slug
                        await create_author(user_id, slug)
            return JSONResponse({"status": "success"})
        except Exception as e:
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


routes = [Route("/", GraphQL(schema, debug=True)), Route("/new-author", WebhookEndpoint)]
app = Starlette(routes=routes, debug=True, on_startup=[start_up], on_shutdown=[shutdown])
