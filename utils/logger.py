import logging
from pathlib import Path

import colorlog

_lib_path = Path(__file__).parents[1]
_leng_path = len(_lib_path.as_posix())


def filter(record: logging.LogRecord):
    # Define `package` attribute with the relative path.
    record.package = record.pathname[_leng_path + 1 :].replace(".py", "")
    record.emoji = (
        "🔍"
        if record.levelno == logging.DEBUG
        else "🖊️"
        if record.levelno == logging.INFO
        else "🚧"
        if record.levelno == logging.WARNING
        else "❌"
        if record.levelno == logging.ERROR
        else "🧨"
        if record.levelno == logging.CRITICAL
        else ""
    )
    return record


# Define the color scheme
color_scheme = {
    "DEBUG": "light_black",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red,bg_white",
}

# Define secondary log colors
secondary_colors = {
    "log_name": {"DEBUG": "blue"},
    "asctime": {"DEBUG": "cyan"},
    "process": {"DEBUG": "purple"},
    "module": {"DEBUG": "light_black,bg_blue"},
    "funcName": {"DEBUG": "light_white,bg_blue"},  # Add this line
}

# Define the log format string
fmt_string = "%(emoji)s%(log_color)s%(package)s.%(funcName)s%(reset)s %(white)s%(message)s"

# Define formatting configuration
fmt_config = {
    "log_colors": color_scheme,
    "secondary_log_colors": secondary_colors,
    "style": "%",
    "reset": True,
}


class MultilineColoredFormatter(colorlog.ColoredFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_colors = kwargs.pop("log_colors", {})
        self.secondary_log_colors = kwargs.pop("secondary_log_colors", {})

    def format(self, record):
        # Add default emoji if not present
        if not hasattr(record, "emoji"):
            record = filter(record)

        message = record.getMessage()
        if "\n" in message:
            lines = message.split("\n")
            first_line = lines[0]
            record.message = first_line
            formatted_first_line = super().format(record)
            formatted_lines = [formatted_first_line]
            for line in lines[1:]:
                formatted_lines.append(line)
            return "\n".join(formatted_lines)
        else:
            return super().format(record)


# Create a MultilineColoredFormatter object for colorized logging
formatter = MultilineColoredFormatter(fmt_string, **fmt_config)

# Create a stream handler for logging output
stream = logging.StreamHandler()
stream.setFormatter(formatter)


def get_colorful_logger(name="main"):
    # Create and configure the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream)
    logger.addFilter(filter)

    return logger


# Set up the root logger with the same formatting
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(stream)
root_logger.addFilter(filter)

ignore_logs = ["_trace", "httpx", "_client", "_trace.atrace", "aiohttp", "_client"]
for lgr in ignore_logs:
    loggr = logging.getLogger(lgr)
    loggr.setLevel(logging.INFO)
