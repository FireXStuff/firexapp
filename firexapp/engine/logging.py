import logging
from _socket import gethostname
from logging.handlers import WatchedFileHandler
import html
from celery.signals import after_setup_task_logger, after_setup_logger
import os
from firexapp.engine.celery import app
from firexkit.resources import get_firex_css_filepath, get_firex_logo_filepath
from firexkit.firexkit_common import JINJA_ENV

RAW_LEVEL_NAME = 'RAW'
PRINT_LEVEL_NAME = 'PRINT'


def add_hostname_to_log_records():
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.hostname = gethostname()
        return record

    logging.setLogRecordFactory(record_factory)


def log_raw(self, message, *args, **kwargs):
    self._log(logging.RAW, message, args, **kwargs)


def log_print(self, message, *args, **kwargs):
    self._log(logging.PRINT, message, args, **kwargs)


def add_custom_log_levels():
    logging.RAW = logging.DEBUG - 5
    logging.addLevelName(logging.RAW, RAW_LEVEL_NAME)
    logging.Logger.raw = log_raw

    logging.PRINT = logging.WARNING + 5
    logging.addLevelName(logging.PRINT, PRINT_LEVEL_NAME)
    logging.Logger.print = log_print

    # Need to add the PRINT custom level to kombu,
    # otherwise, the celery worker barfs at the bootup sequence
    add_print_custom_log_level_to_kombu()


def add_print_custom_log_level_to_kombu():
    from kombu.log import LOG_LEVELS
    LOG_LEVELS.setdefault(PRINT_LEVEL_NAME, logging.PRINT)
    LOG_LEVELS.setdefault(logging.PRINT, PRINT_LEVEL_NAME)
    LOG_LEVELS.setdefault(RAW_LEVEL_NAME, logging.RAW)
    LOG_LEVELS.setdefault(logging.RAW, RAW_LEVEL_NAME)


def html_escape(msg):
    msg = str(msg)
    try:
        return html.escape(msg)
    except TypeError:
        return html.escape(msg.decode('ascii', errors='ignore'))


class FireXFormatter(object):
    def __init__(self, orig_formatter, original_fmt):
        FMT_TYPES = {logging.DEBUG: "<span class='debug'>" + original_fmt + "</span>",
                     logging.ERROR: "<span class='error'>" + original_fmt + "</span>",
                     logging.PRINT: "<span class='print'>" + original_fmt + "</span>",
                     logging.WARNING: "<span class='warning'>" + original_fmt + "</span>",
                     logging.RAW: "%(message)s"}

        self._formatters = {'ORIGINAL': orig_formatter}

        for level, level_fmt in FMT_TYPES.items():
            self._formatters[level] = orig_formatter.__class__(fmt=level_fmt, use_color=False)

    def format(self, record):
        # Take a copy of the stuff we'll override
        keys_to_override = ['msg', 'label', 'span_class', 'span_class_end']
        original_sub_dict = {}
        for key in keys_to_override:
            try:
                original_sub_dict[key] = record.__dict__[key]
            except KeyError:
                pass

        try:
            record.__dict__['label'] = f'<a name="{record.__dict__["label"]}"></a>'
        except KeyError:
            record.__dict__['label'] = ''
        try:
            record.__dict__['span_class'] = "<span class='%s'>" % record.__dict__['span_class']
            record.__dict__['span_class_end'] = '</span>'
        except KeyError:
            record.__dict__['span_class'] = ''
            record.__dict__['span_class_end'] = ''

        if record.levelno != logging.RAW and getattr(record, 'html_escape', True):
            record.msg = html_escape(record.msg)

        result = self._formatters.get(record.levelno, self._formatters['ORIGINAL']).format(record)

        # Restore overridden keys:
        for key in keys_to_override:
            try:
                record.__dict__[key] = original_sub_dict[key]
            except KeyError:
                record.__dict__.pop(key)

        return result


def set_formatter_for_logging_file_handlers(logger, format):
    # Find the WatchedFileHandler
    file_handler = [handler for handler in logger.handlers if isinstance(handler, WatchedFileHandler)][0]
    # set it's formatter to our custom Formatter
    file_handler.setFormatter(FireXFormatter(file_handler.formatter, format))


@after_setup_task_logger.connect
def configure_task_logger(logger, loglevel, logfile, format, colorize, **_kwargs):
    set_formatter_for_logging_file_handlers(logger, format)


@after_setup_logger.connect
def configure_main_logger(logger, loglevel, logfile, format, colorize, **_kwargs):
    set_formatter_for_logging_file_handlers(logger, format)
    # Deduce the worker name from the logfile, which is unfortunate
    worker_name = os.path.splitext(os.path.basename(logfile))[0]
    base_dir = os.path.dirname(logfile)
    logs_url = app.conf.logs_url
    if not logs_url:
        logs_url = os.path.relpath(app.conf.logs_dir, base_dir)

    html_header = JINJA_ENV.get_template('log_template.html').render(
        worker_log=True,
        firex_stylesheet=get_firex_css_filepath(app.conf.resources_dir, relative_from=base_dir),
        logo=get_firex_logo_filepath(app.conf.resources_dir, relative_from=base_dir),
        link_for_logo=app.conf.link_for_logo,
        header_main_title=worker_name,
        firex_id=app.conf.uid,
        logs_dir_url=logs_url)
    logger.raw(html_header)
