import os
from importlib import import_module
from os.path import exists

from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from starlette.applications import Starlette
from starlette.routing import Route

from services.rediscache import redis
from services.schema import resolvers
from services.search import search_service
from services.sentry import start_sentry
from services.viewed import ViewedStorage
from services.webhook import WebhookEndpoint
from settings import DEV_SERVER_PID_FILE_NAME, MODE


import_module('resolvers')
schema = make_executable_schema(load_schema_from_path('schema/'), resolvers)

start_sentry()

async def dev_pid():
    # pid file management
    if MODE == 'development' and not exists(DEV_SERVER_PID_FILE_NAME):
        with open(DEV_SERVER_PID_FILE_NAME, 'w', encoding='utf-8') as f:
            f.write(str(os.getpid()))
    print(f'[main] process started in {MODE} mode')

# main starlette app object with ariadne mounted in root
app = Starlette(
    routes=[
        Route('/', GraphQL(schema, debug=True)),
        Route('/new-author', WebhookEndpoint),
    ],
    on_startup=[
        redis.connect,
        ViewedStorage.init,
        search_service.info,
        dev_pid
    ],
    on_shutdown=[
        redis.disconnect
    ],
    debug=True
)
