import sys
import json
import logging
import os
import argparse
from firexapp.fileregistry import FileRegistry
from shutil import copyfile

from celery.exceptions import NotRegistered

from firexkit.chain import InjectArgs, verify_chain_arguments, InvalidChainArgsException
from firexapp.submit.uid import Uid
from firexapp.submit.arguments import InputConverter, ChainArgException, get_chain_args, find_unused_arguments
from firexapp.plugins import plugin_support_parser
from firexapp.submit.console import setup_console_logging
from firexapp.application import import_microservices, get_app_tasks

logger = setup_console_logging(__name__)


SUBMISSION_FILE_REGISTRY_KEY = 'firex_submission'
FileRegistry().register_file(SUBMISSION_FILE_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'submission.txt'))

ENVIRON_FILE_REGISTRY_KEY = 'env'
FileRegistry().register_file(ENVIRON_FILE_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'environ.json'))


class SubmitBaseApp:
    SUBMISSION_LOGGING_FORMATTER = '[%(asctime)s %(levelname)s] %(message)s'
    DEFAULT_MICROSERVICE = None

    def __init__(self, submission_tmp_file=None):
        self.parser = None
        self.submission_tmp_file = submission_tmp_file
        self.uid = None

    def init_file_logging(self):
        os.umask(0)
        if self.submission_tmp_file:
            logging.basicConfig(filename=self.submission_tmp_file, level=logging.DEBUG, filemode='w',
                                format=self.SUBMISSION_LOGGING_FORMATTER, datefmt="%Y-%m-%d %H:%M:%S")
        self.log_preamble()

    def copy_submission_log(self):
        if self.submission_tmp_file and os.path.isfile(self.submission_tmp_file) and self.uid:
            copyfile(self.submission_tmp_file, FileRegistry().get_file(SUBMISSION_FILE_REGISTRY_KEY, self.uid.logs_dir))

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
        submit_parser.add_argument('--chain', '-chain', help='A comma delimited list of microservices to run',
                                   default=self.DEFAULT_MICROSERVICE)
        submit_parser.set_defaults(func=self.run_submit)
        self.parser = submit_parser
        return self.parser

    def convert_chain_args(self, chain_args) -> dict:
        try:
            return InputConverter.convert(**chain_args)
        except Exception as e:
            logger.error('\nThe arguments you provided firex had the following error:')
            logger.error(e)
            self.main_error_exit_handler()
            sys.exit(-1)

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
        with open(FileRegistry().get_file(ENVIRON_FILE_REGISTRY_KEY, self.uid.logs_dir), 'w') as f:
            json.dump(dict(os.environ), fp=f, skipkeys=True, sort_keys=True, indent=4)

        self.convert_chain_args(chain_args)

        # todo: Concurrency lock
        # todo:   Start Broker

        # IMPORT ALL THE MICROSERVICES
        # ONLY AFTER BROKER HAD STARTED
        try:
            all_tasks = import_microservices(args.plugins)
        except FileNotFoundError as e:
            logger.error("\nError: FireX run failed. File %s is not found." % e)
            self.main_error_exit_handler()
            sys.exit(-1)

        # Post import converters
        self.convert_chain_args(chain_args)

        # check argument applicability to detect useless input arguments
        if not self.validate_argument_applicability(chain_args, args, all_tasks):
            self.main_error_exit_handler()
            sys.exit(-1)

        # locate task objects
        try:
            app_tasks = get_app_tasks(args.chain)
        except NotRegistered as e:
            logger.error("Could not find task %s" % str(e))
            self.main_error_exit_handler()
            sys.exit(-1)

        # validate that all necessary chain args were provided
        c = InjectArgs(**chain_args)
        for t in app_tasks:
            c |= t.s()
        try:
            verify_chain_arguments(c)
        except InvalidChainArgsException as e:
            logger.error(e)
            self.main_error_exit_handler()
            sys.exit(-1)

        # todo:   Start Tracking services
        # todo:   Start Celery
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

    @classmethod
    def validate_argument_applicability(cls, chain_args, args, all_tasks):
        if isinstance(args, argparse.Namespace):
            args = vars(args)
        if isinstance(args, dict):
            args = list(args.keys())

        unused_chain_args = find_unused_arguments(chain_args=chain_args,
                                                  ignore_list=args,
                                                  all_tasks=all_tasks)
        if not unused_chain_args:
            # everything is used. Good job!
            return True

        logger.error("The following arguments are not used by any microservices:")
        for arg in unused_chain_args:
            logger.error("--" + arg)
        return False
