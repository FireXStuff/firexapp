import sys
import json
import logging
import os
import argparse
from shutil import copyfile

from firexapp.submit.uid import Uid
from firexapp.submit.arguments import InputConverter, ChainArgException, get_chain_args
from firexapp.plugins import plugin_support_parser


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

    def copy_submission_log(self):
        if self.submission_tmp_file and os.path.isfile(self.submission_tmp_file) and self.uid:
            copyfile(self.submission_tmp_file, os.path.join(self.uid.logs_dir, "submission.txt"))

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
            self.copy_submission_log()

    def submit(self, args, others):
        chain_args = self.process_other_chain_args(args, others)

        uid = Uid()
        self.uid = uid
        chain_args['uid'] = uid
        logger.info('Logs: %s', uid.logs_dir)

        # Create an env file for debugging
        with open(os.path.join(self.uid.logs_dir, "environ.json"), 'w') as f:
            json.dump(dict(os.environ), fp=f, skipkeys=True, sort_keys=True, indent=4)

        try:
            chain_args = InputConverter.convert(**chain_args)
        except Exception as e:
            logger.error('\nThe arguments you provided firex had the following error:')
            logger.error(e)
            self.main_error_exit_handler()

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

    def main_error_exit_handler(self, expedite=False):
        logger.error('Aborting FireX submission...')


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
