import logging
import colorlog

def get_colorful_logger(name):
    # Define the color scheme
    color_scheme = {
        'DEBUG': 'light_black',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }

    # Create a ColoredFormatter object for colorized logging
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(yellow)s[%(name)s]%(reset)s %(white)s%(message)s",
        log_colors=color_scheme,
        secondary_log_colors={},
        style='%',
        reset=True
    )

    # Create a stream handler for logging output
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)

    # Create and configure the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream)

    return logger
