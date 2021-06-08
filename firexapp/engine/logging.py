import logging

import celery.utils.log
from _socket import gethostname
from logging.handlers import WatchedFileHandler
import html
from celery.signals import after_setup_task_logger, after_setup_logger
import os
from firexapp.engine.celery import app
from firexkit.resources import get_firex_css_filepath, get_firex_logo_filepath
from firexkit.firexkit_common import JINJA_ENV
from celery._state import get_current_task

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


class AddHtmlElementsToLogRecords(logging.Filter):
    def filter(self, record):
        # Add a span class element in the dict
        span_classes = record.levelname.lower()
        try:
            span_class = record.span_class
        except AttributeError:
            pass
        else:
            span_classes += f' {span_class}'
        record.span_class_element = f"<span class='{span_classes}'>"

        # Add a label element if it exists
        try:
            record.label_element += f'<a name="{record.label}"></a>'
        except AttributeError:
            record.label_element = ''

        return True

class FireXFormatter(celery.utils.log.ColorFormatter):
    def __init__(self, fmt):
        new_fmt = '%(span_class_element)s%(label_element)s' + fmt + '</span>'
        super().__init__(fmt=new_fmt, use_color=False)
        self.datefmt = '%m-%d %H:%M:%S %z'

    def format(self, record):
        if record.levelno == logging.RAW:
            original_format = self._style._fmt
            self._style._fmt = '%(message)s'
            mssg = super().format(record)
            self._style._fmt = original_format
            return mssg
        else:
            if getattr(record, 'html_escape', True):
                record.msg = html_escape(record.msg)
            return super().format(record)

class FireXTaskFormatter(FireXFormatter):
    def format(self, record):
        task = get_current_task()
        if task and task.request:
            record.__dict__.update(task_id=task.request.id,
                                   task_name=task.name)
        else:
            record.__dict__.setdefault('task_name', '???')
            record.__dict__.setdefault('task_id', '???')
        return super().format(record)


@after_setup_task_logger.connect
def configure_task_logger(logger, loglevel, logfile, format, colorize, **_kwargs):
    # Find the WatchedFileHandler
    file_handler = [handler for handler in logger.handlers if isinstance(handler, WatchedFileHandler)][0]
    # set it's formatter to our custom Formatter
    file_handler.addFilter(AddHtmlElementsToLogRecords())
    file_handler.setFormatter(FireXTaskFormatter(format))

@after_setup_logger.connect
def configure_main_logger(logger, loglevel, logfile, format, colorize, **_kwargs):
    # Find the WatchedFileHandler
    file_handler = [handler for handler in logger.handlers if isinstance(handler, WatchedFileHandler)][0]
    # set it's formatter to our custom Formatter
    file_handler.addFilter(AddHtmlElementsToLogRecords())
    file_handler.setFormatter(FireXFormatter(format))
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
