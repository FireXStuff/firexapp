import logging
import colorlog
import sys

class DistlibWarningsFilter(logging.Filter):
    def filter(self, record):
        pathname = record.pathname
        return not pathname.endswith('distlib/metadata.py') and not pathname.endswith('distlib/database.py')


def setup_console_logging(module='',
                          stdout_logging_level=logging.INFO,
                          console_logging_formatter='%(green)s[%(asctime)s]%(reset)s %(log_color)s%(message)s',
                          console_datefmt="%H:%M:%S",
                          stderr_logging_level=logging.ERROR,
                          module_logger_logging_level=logging.DEBUG):

    module_logger = logging.getLogger(module)
    module_logger.setLevel(module_logger_logging_level)

    formatter = colorlog.ColoredFormatter(fmt=console_logging_formatter,
                                          datefmt=console_datefmt,
                                          log_colors={'DEBUG': 'cyan',
                                                      'INFO': 'bold',
                                                      'WARNING': 'yellow',
                                                      'ERROR': 'red',
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

    console_stdout = logging.StreamHandler(sys.stdout)
    console_stdout.setLevel(stdout_logging_level)
    console_stdout.setFormatter(formatter)
    log_filter = LogLevelFilter(stderr_logging_level)
    console_stdout.addFilter(log_filter)
    console_stdout.addFilter(DistlibWarningsFilter())

    console_stderr = logging.StreamHandler()
    console_stderr.setLevel(stderr_logging_level)
    console_stderr.setFormatter(formatter)
    module_logger.addHandler(console_stdout)
    module_logger.addHandler(console_stderr)

    return module_logger
