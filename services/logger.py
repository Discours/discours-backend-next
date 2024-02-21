import logging
import colorlog

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
}

# Define the log format string
fmt_string = "%(log_color)s%(levelname)s: %(log_color)s[%(module)s]%(reset)s %(white)s%(message)s"

# Define formatting configuration
fmt_config = {
    "log_colors": color_scheme,
    "secondary_log_colors": secondary_colors,
    "style": "%",
    "reset": True,
}


class MultilineColoredFormatter(colorlog.ColoredFormatter):
    def format(self, record):
        # Check if the message is multiline
        if record.getMessage() and "\n" in record.getMessage():
            # Split the message into lines
            lines = record.getMessage().split("\n")
            formatted_lines = []
            for line in lines:
                # Format each line with the provided format
                formatted_lines.append(super().format(record))
            # Join the formatted lines
            return "\n".join(formatted_lines)
        else:
            # If not multiline or no message, use the default formatting
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

    return logger


# Set up the root logger with the same formatting
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(stream)
