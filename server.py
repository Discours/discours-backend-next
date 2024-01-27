from granian.constants import Interfaces
from granian.server import Granian


if __name__ == '__main__':
    print('[server] started')

    granian_instance = Granian(
        'main:app',
        address='0.0.0.0', # noqa S104
        port=8000,
        workers=2,
        threads=2,
        websockets=False,
        interface=Interfaces.ASGI
    )
    granian_instance.serve()
