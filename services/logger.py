import logging
import colorlog

def get_colorful_logger(name):
    # Создаем объект форматирования для цветовой разметки
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(yellow)s[%(name)s]%(reset)s %(white)s%(message)s",
        log_colors={
            'DEBUG': 'light_black',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )

    # Создаем поток вывода для записи журнала
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)

    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream)

    return logger
