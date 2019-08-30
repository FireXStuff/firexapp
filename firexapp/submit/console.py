import logging
import coloredlogs


class DistlibWarningsFilter(logging.Filter):
    def filter(self, record):
        pathname = record.pathname
        return not pathname.endswith('distlib/metadata.py') and not pathname.endswith('distlib/database.py')


def setup_console_logging(module='',
                          console_logging_level=logging.INFO,
                          console_logging_formatter='[%(asctime)s] %(message)s',
                          console_datefmt="%H:%M:%S"):
    module_logger = logging.getLogger(module)
    # Install STDOUT and STDERR StremHandlers with Colored Formatting
    coloredlogs.install(logger=module_logger,
                        fmt=console_logging_formatter,
                        datefmt=console_datefmt,
                        level=console_logging_level)
    return module_logger
