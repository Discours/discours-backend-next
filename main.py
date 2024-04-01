import os
import sentry_sdk
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from importlib import import_module
from os.path import exists

from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.middleware import Middleware

from services.rediscache import redis
from services.schema import resolvers
from services.viewed import ViewedStorage
from services.webhook import WebhookEndpoint
from settings import DEV_SERVER_PID_FILE_NAME, MODE

import_module('resolvers')
schema = make_executable_schema(load_schema_from_path('schema/'), resolvers)

# Initialize GlitchTip SDK with DSN from environment variable
GLITCHTIP_DSN = os.getenv('GLITCHTIP_DSN')
sentry_sdk.init(
    dsn=GLITCHTIP_DSN,
    traces_sample_rate=1.0
)

async def start():
    if MODE == 'development':
        if not exists(DEV_SERVER_PID_FILE_NAME):
            # pid file management
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
        # search_service.info,
        # start_sentry,
        start,
    ],
    on_shutdown=[redis.disconnect],
    middleware=[Middleware(SentryAsgiMiddleware)],
    debug=True,
)