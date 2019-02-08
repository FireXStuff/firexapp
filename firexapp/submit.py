import sys
import json
import logging
import os
import re
import datetime
import pytz
import argparse
import tempfile
from shutil import copyfile
from getpass import getuser

from firexapp.plugins import plugin_support_parser
from firexkit.argument_conversion import ConverterRegister


class SubmitBaseApp:
    SUBMISSION_LOGGING_FORMATTER = '[%(asctime)s %(levelname)s] %(message)s'
    CONSOLE_LOGGING_FORMATTER = '[%(asctime)s] %(message)s'

    def __init__(self, submission_tmp_file=None):
        self.parser = None
        self.submission_tmp_file = submission_tmp_file
        self.uid = None

    def init_file_logging(self):
        os.umask(0)
        if self.submission_tmp_file:
            if self.submission_tmp_file:
                logging.basicConfig(filename=self.submission_tmp_file, level=logging.DEBUG, filemode='w',
                                    format=self.SUBMISSION_LOGGING_FORMATTER, datefmt="%Y-%m-%d %H:%M:%S")
        self.log_preamble()

    def log_preamble(self):
        """Overridable method to allow a firex application to log on startup"""
        pass

    def create_submit_parser(self, sub_parser):
        if self.parser:
            return self.parser
        submit_parser = sub_parser.add_parser("submit",
                                              help="This tool invokes a fireX run and is the most common use of "
                                                   "firex. Check out our documentation for common usage patterns",
                                              parents=[plugin_support_parser],
                                              formatter_class=argparse.RawDescriptionHelpFormatter)

        submit_parser.set_defaults(func=self.run_submit)
        self.parser = submit_parser
        return self.parser

    def run_submit(self, args, others):
        try:
            self.init_file_logging()
            return self.submit(args, others)
        finally:
            if self.submission_tmp_file and os.path.isfile(self.submission_tmp_file) and self.uid:
                copyfile(self.submission_tmp_file, os.path.join(self.uid.logs_dir, "submission.txt"))

    def submit(self, args, others):
        chain_args = self.process_other_chain_args(args, others)

        uid = Uid()
        self.uid = uid
        chain_args['uid'] = uid
        logger.info('Logs: %s', uid.logs_dir)

        # Create an env file
        with open(os.path.join(uid.logs_dir, "environ.json"), 'w') as f:
            json.dump(dict(os.environ), fp=f, skipkeys=True, sort_keys=True, indent=4)
        try:
            chain_args = InputConverter.convert(**chain_args)
        except Exception as e:
            logger.error('\nThe arguments you provided firex had the following error:')
            logger.error(e)
            self.parser.exit(-1, 'Aborting...')
            # todo: Main exit handler

        # todo: Concurrency lock
        # todo:   Start Broker
        # todo:   Import all microservices
        # todo:   post import converters
        # todo:   check argument applicability
        # todo:   Verify chain
        # todo:   Execute chain
        # todo: do sync

    def process_other_chain_args(self, args, other_args)-> {}:
        chain_args = {}
        try:
            chain_args = get_chain_args(other_args)
        except ChainArgException as e:
            logger.error(str(e))
            self.parser.exit(-1,  'Aborting...')

        return chain_args


class Uid(object):
    def __init__(self, identifier=None):
        self.timestamp = datetime.datetime.now(tz=pytz.utc)
        self.user = getuser()
        if identifier:
            self.identifier = identifier
        else:
            self.identifier = 'FireX-%s-%s-%s' % (self.user, self.timestamp.strftime("%y%m%d-%H%M%S"), os.getpid())
        self._logs_dir = None

    @property
    def base_logging_dir(self):
        return tempfile.gettempdir()

    @property
    def logs_dir(self):
        if not self._logs_dir:
            self._logs_dir = self.create_logs_dir()
        return self._logs_dir

    def create_logs_dir(self):
        path = os.path.join(self.base_logging_dir, self.identifier)
        os.makedirs(path, 0o766)
        return path

    def __str__(self):
        return self.identifier

    def __repr__(self):
        return self.identifier

    def __eq__(self, other):
        return str(other) == self.identifier


def get_chain_args(other_args):
    chain_arguments = {}
    # Create arguments list for the chain
    it = iter(other_args)
    no_value_exception = None
    for x in it:
        if not x.startswith('-'):
            if no_value_exception:
                # the error was earlier
                raise no_value_exception
            raise ChainArgException('Error: Argument should start with a proper dash (- or --)\n%s' % x)

        try:
            value = next(it)
            if str(value).startswith("-"):
                # there might be an error. we'll find out later
                no_value_exception = ChainArgException(
                    'Error: Arguments must have an accompanying value\n%s' % x)
        except StopIteration:
            raise ChainArgException('Error: Arguments must have an accompanying value\n%s' % x)

        key = x.lstrip('-')
        if not re.match('^[A-Za-z].*', key):
            raise ChainArgException('Error: Argument should start with a letter\n%s' % key)
        chain_arguments[key] = value
    return chain_arguments


class ChainArgException(Exception):
    pass


class InputConverter:
    """
    This class uses a singleton object design to store converters which parse the cli arguments. Converter functions
    are stored into the singleton InputConverter object by adding the @register decorator to the top of each desired
    function.
    """
    _global_instance = None
    pre_load_was_run = False

    @classmethod
    def instance(cls) -> ConverterRegister:  # usd by tests only
        """Used for unit testing only"""
        if cls._global_instance is None:
            cls._global_instance = ConverterRegister()
        return cls._global_instance

    @classmethod
    def register(cls, *args):
        """
        Registers a callable object to be run during conversion. The callable should take in kwargs, and return a dict
        with any changes to the input arguments, or None if no changes are necessary.

        :Example single argument converter:

        @InputConverter.register
        @SingleArgDecorator('something')
        def convert_something(arg_value):
            arg_value = arg_value.upper()
            return arg_value

        :Optionally, dependencies can defined at registration:

        @InputConverter.register('other_converter', 'and_another_converter')
        @SingleArgDecorator('something')
        def convert_something(arg_value):
            arg_value = arg_value.upper()
            return arg_value

        Conversion occurs on two occasions, before microservices are loaded, or after. You can explicitly mark a
        converter to run pre-loading or post-loading of the ALL microservices by passing True (pre) or False (post)
        during registration. This design is used in the spirit of failing fast, providing early failure of runs before
        the bulk of microservices are imported. If bool is not provided, it will register to run pre unless loading has
        already occurred.

        @InputConverter.register('other_converter', False)
        @SingleArgDecorator('something')
        def convert_something(arg_value):
            ...
            return arg_value

        When a conversion fails the given function can simply call raise to instruct the user how to correct their
        inputs.
        """
        for arg in args:
            if not isinstance(arg, bool):
                continue
            if arg and cls.pre_load_was_run:
                raise Exception("Pre-microservice load conversion has already been run. "
                                "You can only register post load")
            break
        else:
            args = args + (not cls.pre_load_was_run,)
        return cls.instance().register(*args)

    @classmethod
    def convert(cls, pre_load=None, **kwargs) -> dict:
        """
        Activates conversion. kwargs provided are passed to any registered converter. This function should be called
        twice, and only twice. Once with initially loaded converters, and then with the secondary ones.

        :param pre_load: Used for testing. preload is defaulted to None and will auto populate
        """
        # Auto set whether this is preload, unless explicitly specified
        pre_load = not cls.pre_load_was_run if pre_load is None else pre_load
        if pre_load and cls.pre_load_was_run:
                raise Exception("Pre-microservice conversion was already run")

        cls.pre_load_was_run = True
        return cls.instance().convert(pre_task=pre_load, **kwargs)


@InputConverter.register
def convert_booleans(kwargs):
    """Converts standard true/false/none values to bools and None"""
    for key, value in kwargs.items():
        if not isinstance(value, str):
            continue
        if value.upper() == 'TRUE':
            value = True
        elif value.upper() == 'FALSE':
            value = False
        elif value.upper() == 'NONE':
            value = None
        kwargs[key] = value
    return kwargs


def setup_console_logging(module=''):
    formatter = logging.Formatter(SubmitBaseApp.CONSOLE_LOGGING_FORMATTER, "%H:%M:%S")
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


logger = setup_console_logging(__name__)
