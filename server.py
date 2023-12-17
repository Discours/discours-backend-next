import sys

import uvicorn
from uvicorn.main import logger

from settings import PORT

log_settings = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(message)s",
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO"},
        "uvicorn.error": {"level": "INFO", "handlers": ["default"], "propagate": True},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
}

local_headers = [
    ("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD"),
    ("Access-Control-Allow-Origin", "https://localhost:3000"),
    (
        "Access-Control-Allow-Headers",
        "DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization",
    ),
    ("Access-Control-Expose-Headers", "Content-Length,Content-Range"),
    ("Access-Control-Allow-Credentials", "true"),
]


def exception_handler(_et, exc, _tb):
    logger.error(..., exc_info=(type(exc), exc, exc.__traceback__))


if __name__ == "__main__":
    sys.excepthook = exception_handler
    if "dev" in sys.argv:
        import os

        os.environ["MODE"] = "development"
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, proxy_headers=True, server_header=True)
