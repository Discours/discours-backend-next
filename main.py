import os
from importlib import import_module
from os.path import exists
from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from starlette.applications import Starlette
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

        sentry_sdk.init(SENTRY_DSN)
    except Exception as e:
        print("[sentry] init error")
        print(e)


async def shutdown():
    await redis.disconnect()


app = Starlette(debug=True, on_startup=[start_up], on_shutdown=[shutdown])
app.mount("/", GraphQL(schema, debug=True))
