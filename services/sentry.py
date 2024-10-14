import sentry_sdk
from sentry_sdk.integrations.ariadne import AriadneIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
import logging

from settings import GLITCHTIP_DSN

logger = logging.getLogger(__name__)
# Настройка логирования для отправки логов в Sentry
sentry_logging_handler = sentry_sdk.integrations.logging.SentryHandler(level=logging.WARNING)
logger.addHandler(sentry_logging_handler)
logger.setLevel(logging.DEBUG)  # Более подробное логирование

def start_sentry():
    try:
        logger.info("[services.sentry] Sentry init started...")
        sentry_sdk.init(
            dsn=GLITCHTIP_DSN,
            traces_sample_rate=1.0,  # Захват 100% транзакций
            profiles_sample_rate=1.0,  # Профилирование 100% транзакций
            enable_tracing=True,
            integrations=[StarletteIntegration(), AriadneIntegration(), SqlalchemyIntegration()],
            send_default_pii=True,  # Отправка информации о пользователе (PII)
        )
        logger.info("[services.sentry] Sentry initialized successfully.")
    except Exception as e:
        logger.warning("[services.sentry] Failed to initialize Sentry", exc_info=True)


