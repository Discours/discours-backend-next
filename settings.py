import sys
from os import environ

PORT = 8000
DB_URL = (
    environ.get("DATABASE_URL", "").replace("postgres://", "postgresql://")
    or environ.get("DB_URL", "").replace("postgres://", "postgresql://")
    or "sqlite:///discoursio-db.sqlite3"
)
REDIS_URL = environ.get("REDIS_URL") or "redis://127.0.0.1"
API_BASE = environ.get("API_BASE") or ""
AUTH_URL = environ.get("AUTH_URL") or ""
GLITCHTIP_DSN = environ.get("GLITCHTIP_DSN")
DEV_SERVER_PID_FILE_NAME = "dev-server.pid"
MODE = "development" if "dev" in sys.argv else "production"

ADMIN_SECRET = environ.get("AUTH_SECRET") or "nothing"
WEBHOOK_SECRET = environ.get("WEBHOOK_SECRET") or "nothing-else"