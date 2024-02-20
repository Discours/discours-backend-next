import logging
import colorlog


# Define the color scheme
color_scheme = {
    'DEBUG': 'light_black',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'red,bg_white',
}

# Define secondary log colors
secondary_colors = {
    'log_name': {'DEBUG': 'blue'},
    'asctime': {'DEBUG': 'cyan'},
    'process': {'DEBUG': 'purple'},
    'module': {'DEBUG': 'light_black,bg_blue'},
}

# Create a ColoredFormatter object for colorized logging
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)s: %(log_color)s[%(module)s]%(reset)s %(white)s%(message)s",
    log_colors=color_scheme,
    secondary_log_colors=secondary_colors,  # Set secondary log colors
    style='%',
    reset=True
)

def get_colorful_logger(name='main'):

    # Create a stream handler for logging output
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)

    # Create and configure the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream)

    return logger

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)  # Set the log level
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
root_logger.addHandler(stream_handler)
