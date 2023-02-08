import sys
import logging
import colorlog
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from firexkit.result import ChainInterruptedException
import warnings

# BeautifulSoup thinks we're giving it an URL because there is an URL in msg.
# Not good. Keep stderr clean by ignoring this warning.
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

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
        format_orig = self._style._fmt
        override_exc_text = None
        if record.exc_text and not record.exc_info and hasattr(record, 'task_id'):
            # This is a serialized exception, and we are not interested in showing the traceback on the console,
            # just the string.
            override_exc_text = record.exc_text
            record.exc_text = None
        try:
            record.msg = BeautifulSoup(record.msg, 'html.parser').get_text()
        except Exception:
            pass
        prefixes = getattr(record, 'prefixes', True)
        if not prefixes:
            # Use a minimal format without the hostname and time
            self._style._fmt = '%(log_color)s%(message)s'
        msg = super(FireXColoredConsoleFormatter, self).format(record)
        # Restore original formats
        self._style._fmt = format_orig
        if override_exc_text:
            # Restore exc_text
            record.exc_text = override_exc_text
        return msg


class RetryFilter(logging.Filter):
    def filter(self, record):
        return 'Retry in' not in record.getMessage()


class ChainInterruptedExceptionFilter(logging.Filter):
    def filter(self, record):
        return ChainInterruptedException.__name__ not in record.getMessage()


def setup_console_logging(module=None,
                          stdout_logging_level=logging.INFO,
                          console_logging_formatter='%(green)s[%(asctime)s]%(reset)s[%(hostname)s] %(log_color)s%(message)s',
                          console_datefmt="%H:%M:%S",
                          stderr_logging_level=logging.ERROR,
                          module_logger_logging_level=None):

    formatter = FireXColoredConsoleFormatter(fmt=console_logging_formatter,
                                             datefmt=console_datefmt,
                                             log_colors={'DEBUG': 'cyan',
                                                         'INFO': 'bold',
                                                         'WARNING': 'yellow',
                                                         'ERROR': 'bold_red',
                                                         'CRITICAL': 'red,bg_white'})

    class LogLevelFilter(logging.Filter):
        """Filters (lets through) all messages with level < LEVEL"""

        def __init__(self, level):
            self.level = level
            super(LogLevelFilter, self).__init__()

        def filter(self, record):
            # "<" instead of "<=": since logger.setLevel is inclusive, this should
            # be exclusive
            return record.levelno < self.level

    if module == "__main__":
        # For program entry point, use root logger
        module_logger = logging.getLogger()
        if module_logger_logging_level is None:
            module_logger_logging_level = logging.DEBUG
        module_logger.setLevel(module_logger_logging_level)

        global console_stdout
        global console_stderr

        if not console_stdout:
            # This setup hasn't been done before

            from firexapp.engine.logging import add_hostname_to_log_records
            add_hostname_to_log_records()

            console_stdout = logging.StreamHandler(sys.stdout)
            console_stdout.setLevel(stdout_logging_level)
            console_stdout.setFormatter(formatter)
            console_stdout.addFilter(LogLevelFilter(stderr_logging_level))
            console_stdout.addFilter(DistlibWarningsFilter())
            console_stdout.addFilter(RequeueingUndeliverableFilter())
            module_logger.addHandler(console_stdout)

            console_stderr = logging.StreamHandler()
            console_stderr.setLevel(stderr_logging_level)
            console_stderr.setFormatter(formatter)
            console_stderr.addFilter(RetryFilter())
            console_stderr.addFilter(ChainInterruptedExceptionFilter())
            module_logger.addHandler(console_stderr)
    else:
        # For submodules, create sub-loggers
        module_logger = logging.getLogger(module)
        if module_logger_logging_level is None:
            module_logger_logging_level = logging.NOTSET
        module_logger.setLevel(module_logger_logging_level)

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
