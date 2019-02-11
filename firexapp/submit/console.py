import sys
import logging

CONSOLE_LOGGING_FORMATTER = '[%(asctime)s] %(message)s'


def setup_console_logging(module=''):
    formatter = logging.Formatter(CONSOLE_LOGGING_FORMATTER, "%H:%M:%S")
    module_logger = logging.getLogger(module)
    module_logger.setLevel(logging.DEBUG)

    class LogLevelFilter(logging.Filter):
        """Filters (lets through) all messages with level < LEVEL"""

        def __init__(self, level):
            self.level = level
            super(LogLevelFilter, self).__init__()

        def filter(self, record):
            # "<" instead of "<=": since logger.setLevel is inclusive, this should
            # be exclusive
            return record.levelno < self.level

    console_stdout = logging.StreamHandler(sys.stdout)
    console_stdout.setLevel(logging.INFO)
    console_stdout.setFormatter(formatter)
    log_filter = LogLevelFilter(logging.ERROR)
    console_stdout.addFilter(log_filter)

    console_stderr = logging.StreamHandler()
    console_stderr.setLevel(logging.ERROR)
    console_stderr.setFormatter(formatter)
    module_logger.addHandler(console_stdout)
    module_logger.addHandler(console_stderr)
    return module_logger
