import sys
from pathlib import Path

from granian.constants import Interfaces
from granian.log import LogLevels
from granian.server import Granian

from settings import PORT
from utils.logger import root_logger as logger

if __name__ == "__main__":
    logger.info("started")

    try:
        granian_instance = Granian(
            "main:app",
            address="0.0.0.0",
            port=PORT,
            interface=Interfaces.ASGI,
            threads=4,
            websockets=False,
            log_level=LogLevels.debug,
            backlog=2048,
        )

        if "dev" in sys.argv:
            logger.info("dev mode, building ssl context")
            granian_instance.build_ssl_context(cert=Path("localhost.pem"), key=Path("localhost-key.pem"), password=None)
        granian_instance.serve()
    except Exception as error:
        logger.error(f"Granian error: {error}", exc_info=True)
        raise
    finally:
        logger.info("stopped")
