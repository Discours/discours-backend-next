from os import environ

PORT = 8080
DB_URL = (
    environ.get("DATABASE_URL", "").replace("postgres://", "postgresql://")
    or environ.get("DB_URL", "").replace("postgres://", "postgresql://")
    or "postgresql://postgres@localhost:5432/discoursio"
)
REDIS_URL = environ.get("REDIS_URL") or "redis://127.0.0.1"
API_BASE = environ.get("API_BASE") or ""
AUTH_URL = environ.get("AUTH_URL") or ""
MODE = environ.get("MODE") or "production"
SENTRY_DSN = environ.get("SENTRY_DSN")
DEV_SERVER_PID_FILE_NAME = "dev-server.pid"
