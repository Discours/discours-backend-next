import sentry_sdk
from sentry_sdk.integrations.ariadne import AriadneIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration

from settings import GLITCHTIP_DSN


def start_sentry():
    # sentry monitoring
    try:
        sentry_sdk.init(
            GLITCHTIP_DSN,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            traces_sample_rate=1.0,
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production.
            profiles_sample_rate=1.0,
            enable_tracing=True,
            integrations=[
                StarletteIntegration(),
                AriadneIntegration(),
                SqlalchemyIntegration(),
            ],
        )
    except Exception as e:
        print("[services.sentry] init error")
        print(e)
