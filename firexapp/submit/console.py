import sys
import re
import logging
CONSOLE_LOGGING_FORMATTER = '[%(asctime)s] %(message)s'

console_stdout = None


class DistlibWarningsFilter(logging.Filter):
    def filter(self, record):
        pathname = record.pathname
        return not pathname.endswith('distlib/metadata.py') and not pathname.endswith('distlib/database.py')


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
    log_filter = LogLevelFilter(logging.ERROR)
    console_stdout.addFilter(log_filter)
    console_stdout.addFilter(DistlibWarningsFilter())

    console_stderr = logging.StreamHandler()
    console_stderr.setLevel(logging.ERROR)
    console_stderr.setFormatter(formatter)
    module_logger.addHandler(console_stdout)
    module_logger.addHandler(console_stderr)
    return module_logger


def set_console_log_level(log_level):
    global console_stdout
    console_stdout.setLevel(log_level)


def add_task_filter_to_console():
    # For task-"level" logging, set to DEBUG and filter out everything below the previous log level except the lines
    # matching filter_re
    class TaskFilter(logging.Filter):
        """Filters out non-task-related log entries"""
        filter_re = r'=+(STARTED: [^=]+)=+|\*+(COMPLETED: [^*]+)\*+'

        def __init__(self):
            self.filter_cre = re.compile(self.filter_re)
            self.filter_log_level = console_stdout.level
            super(TaskFilter, self).__init__()

        def filter(self, record):
            # Include messages of level equal or higher to the saved log level of the handler
            if record.levelno >= self.filter_log_level:
                return True
            # Skip non-text messages
            if not isinstance(record.msg, str):
                return False
            # For messages matching the RE, extract the grouping and subst the msg for that text.
            match = self.filter_cre.search(record.msg)
            if match:
                record.msg = match.group(1) if match.group(1) else match.group(2)
                return True
            return False

    log_filter = TaskFilter()
    console_stdout.addFilter(log_filter)
    set_console_log_level(logging.DEBUG)
