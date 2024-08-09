import subprocess

from granian.constants import Interfaces
from granian.server import Granian

from settings import PORT
from utils.logger import root_logger as logger


def is_docker_container_running(name):
    cmd = ["docker", "ps", "-f", f"name={name}"]
    output = subprocess.run(cmd, capture_output=True, text=True).stdout
    logger.info(output)
    return name in output


if __name__ == "__main__":
    logger.info("started")

    granian_instance = Granian(
        "main:app",
        address="0.0.0.0",  # noqa S104
        port=PORT,
        threads=4,
        websockets=False,
        interface=Interfaces.ASGI,
    )
    granian_instance.serve()
