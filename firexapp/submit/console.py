import sys
import logging
import colorlog
from bs4 import BeautifulSoup

console_stdout = None
console_stderr = None


class RequeueingUndeliverableFilter(logging.Filter):
    def filter(self, record):
        return 'Requeuing undeliverable message for queue' not in record.getMessage()


class DistlibWarningsFilter(logging.Filter):
    def filter(self, record):
        pathname = record.pathname
        return not pathname.endswith('distlib/metadata.py') and not pathname.endswith('distlib/database.py')


class FireXColoredConsoleFormatter(colorlog.TTYColoredFormatter):
    def format(self, record):
        override_exc_text = None
        if record.exc_text and not record.exc_info and hasattr(record, 'task_id'):
            # This is a serialized exception, and we are not interested in showing the traceback on the console,
            # just the string.
            override_exc_text = record.exc_text
            record.exc_text = None
        try:
            record.msg = BeautifulSoup(record.msg, 'lxml').get_text()
        except Exception:
            pass
        msg = super(FireXColoredConsoleFormatter, self).format(record)
        if override_exc_text:
            # Restore exc_text
            record.exc_text = override_exc_text
        return msg


class RetryFilter(logging.Filter):
    def filter(self, record):
        return 'Retry in' not in record.getMessage()


def setup_console_logging(module=None,
                          stdout_logging_level=logging.INFO,
                          console_logging_formatter='%(green)s[%(asctime)s]%(reset)s[%(hostname)s] %(log_color)s%(message)s',
                          console_datefmt="%H:%M:%S",
                          stderr_logging_level=logging.ERROR,
                          module_logger_logging_level=None):
    global console_stdout
    global console_stderr

    formatter = FireXColoredConsoleFormatter(fmt=console_logging_formatter,
                                             datefmt=console_datefmt,
                                             log_colors={'DEBUG': 'cyan',
                                                         'INFO': 'bold',
                                                         'WARNING': 'yellow',
                                                         'ERROR': 'bold_red',
                                                         'CRITICAL': 'red,bg_white'})

    if module == "__main__":
        # For program entry point, use root logger
        module_logger = logging.getLogger()
        # noinspection PyUnresolvedReferences
        if module_logger_logging_level is None:
            module_logger_logging_level = logging.DEBUG
        module_logger.setLevel(module_logger_logging_level)
    else:
        # For submodules, create sub-loggers
        module_logger = logging.getLogger(module)
        if module_logger_logging_level is None:
            module_logger_logging_level = logging.NOTSET
        module_logger.setLevel(module_logger_logging_level)
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
    console_stdout.setLevel(stdout_logging_level)
    console_stdout.setFormatter(formatter)
    console_stdout.addFilter(LogLevelFilter(stderr_logging_level))
    console_stdout.addFilter(DistlibWarningsFilter())
    console_stdout.addFilter(RequeueingUndeliverableFilter())

    console_stderr = logging.StreamHandler()
    console_stderr.setLevel(stderr_logging_level)
    console_stderr.setFormatter(formatter)
    console_stderr.addFilter(RetryFilter())

    module_logger.addHandler(console_stdout)
    module_logger.addHandler(console_stderr)
    return module_logger


def set_console_log_level(log_level):
    global console_stdout
    global console_stderr
    console_stdout.setLevel(log_level)
    if log_level == logging.CRITICAL:
        console_stderr.setLevel(log_level)


def add_filter_to_console(log_filter):
    assert isinstance(log_filter, logging.Filter)
    console_stdout.addFilter(log_filter)
    set_console_log_level(logging.DEBUG)
