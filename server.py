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
        
        granian_instance.serve()
    except Exception as error:
        logger.error(f"Granian error: {error}", exc_info=True)
        raise
    finally:
        logger.info("stopped")
