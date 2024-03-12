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
    'funcName': {'DEBUG': 'light_white,bg_blue'},  # Add this line
}

# Define the log format string
fmt_string = '%(log_color)s%(levelname)s: %(log_color)s[%(module)s.%(funcName)s]%(reset)s %(white)s%(message)s'

# Define formatting configuration
fmt_config = {
    'log_colors': color_scheme,
    'secondary_log_colors': secondary_colors,
    'style': '%',
    'reset': True,
}


class MultilineColoredFormatter(colorlog.ColoredFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_colors = kwargs.pop('log_colors', {})
        self.secondary_log_colors = kwargs.pop('secondary_log_colors', {})

    def format(self, record):
        message = record.getMessage()
        if '\n' in message:
            lines = message.split('\n')
            record.message = lines[0]  # Keep only the first line in the record
            formatted_lines = [super().format(record)]  # Format the first line with prefix
            for line in lines[1:]:
                record.message = line  # Set the message to the remaining lines one by one
                formatted_lines.append(self.format_secondary_line(record))  # Format without prefix
            return '\n'.join(formatted_lines)
        else:
            return super().format(record)

    def format_secondary_line(self, record):
        msg = record.getMessage()
        log_color = self.log_colors.get(record.levelname, 'reset')
        formatted_msg = []

        # Since there are no secondary log colors in the subsequent lines,
        # we apply the log_color to each part of the message.
        for part in msg.split(' '):
            formatted_msg.append(f"{log_color}{part}{self.reset}")

        return ' '.join(formatted_msg)




# Create a MultilineColoredFormatter object for colorized logging
formatter = MultilineColoredFormatter(fmt_string, **fmt_config)

# Create a stream handler for logging output
stream = logging.StreamHandler()
stream.setFormatter(formatter)


def get_colorful_logger(name='main'):
    # Create and configure the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream)

    return logger


# Set up the root logger with the same formatting
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(stream)

ignore_logs = ['_trace', 'httpx', '_client', '_trace.atrace', 'aiohttp', '_client']
for lgr in ignore_logs:
    loggr = logging.getLogger(lgr)
    loggr.setLevel(logging.INFO)
