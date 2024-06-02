import logging

import colorlog

# Define the color scheme
color_scheme = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red,bg_white",
    "DEFAULT": "white",
}

# Define secondary log colors
secondary_colors = {
    "log_name": {"DEBUG": "blue"},
    "asctime": {"DEBUG": "cyan"},
    "process": {"DEBUG": "purple"},
    "module": {"DEBUG": "cyan,bg_blue"},
    "funcName": {"DEBUG": "light_white,bg_blue"},
}

# Define the log format string
fmt_string = "%(log_color)s%(levelname)s: %(log_color)s[%(module)s.%(funcName)s]%(reset)s %(white)s%(message)s"

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

# Set up the root logger with the same formatting
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(stream)

ignore_logs = [
    "_trace",
    "httpx",
    "_client",
    "_trace.atrace",
    "aiohttp",
    "_client",
    "._make_request",
    "._log_request_response"
]
for lgr in ignore_logs:
    loggr = logging.getLogger(lgr)
    loggr.setLevel(logging.INFO)
