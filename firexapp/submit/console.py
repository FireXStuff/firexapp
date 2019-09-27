import sys
import logging
CONSOLE_LOGGING_FORMATTER = '[%(asctime)s] %(message)s'

console_stdout = None


class DistlibWarningsFilter(logging.Filter):
    def filter(self, record):
        pathname = record.pathname
        return not pathname.endswith('distlib/metadata.py') and not pathname.endswith('distlib/database.py')


class RetryFilter(logging.Filter):
    def filter(self, record):
        return not record.getMessage().startswith('Retry in')


def setup_console_logging(module=None):
    global console_stdout

    formatter = logging.Formatter(CONSOLE_LOGGING_FORMATTER, "%H:%M:%S")
    if module == "__main__":
        # For program entry point, use root logger
        module_logger = logging.getLogger()
        # noinspection PyUnresolvedReferences
        module_logger.setLevel(logging.DEBUG)
    else:
        # For submodules, create sub-loggers
        module_logger = logging.getLogger(module)
        module_logger.setLevel(logging.NOTSET)
        return module_logger

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
    console_stdout.addFilter(LogLevelFilter(logging.ERROR))
    console_stdout.addFilter(DistlibWarningsFilter())

    console_stderr = logging.StreamHandler()
    console_stderr.setLevel(logging.ERROR)
    console_stderr.setFormatter(formatter)
    console_stderr.addFilter(RetryFilter())

    module_logger.addHandler(console_stdout)
    module_logger.addHandler(console_stderr)
    return module_logger


def set_console_log_level(log_level):
    global console_stdout
    console_stdout.setLevel(log_level)


def add_filter_to_console(log_filter):
    assert isinstance(log_filter, logging.Filter)
    console_stdout.addFilter(log_filter)
    set_console_log_level(logging.DEBUG)
