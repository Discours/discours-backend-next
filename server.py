from granian.constants import Interfaces
from granian.server import Granian
from services.logger import root_logger as logger

if __name__ == '__main__':
    logger.info('started')

    granian_instance = Granian(
        'main:app',
        address='0.0.0.0',  # noqa S104
        port=8000,
        threads=4,
        websockets=False,
        interface=Interfaces.ASGI,
    )
    granian_instance.serve()
