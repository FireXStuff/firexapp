import re
import sys
import json
import logging
import os
import argparse
import time
import traceback
from celery.signals import celeryd_init, worker_ready
from shutil import copyfile
from contextlib import contextmanager

from celery.exceptions import NotRegistered
from firexapp.engine.logging import add_hostname_to_log_records

from firexkit.result import wait_on_async_results, disable_async_result, find_all_unsuccessful, ChainRevokedException, \
    ChainInterruptedException, mark_queues_ready
from firexkit.chain import InjectArgs, verify_chain_arguments, InvalidChainArgsException
from firexapp.fileregistry import FileRegistry
from firexapp.submit.uid import Uid
from firexapp.submit.arguments import InputConverter, ChainArgException, get_chain_args, find_unused_arguments
from firexapp.submit.tracking_service import get_tracking_services, get_service_name
from firexapp.plugins import plugin_support_parser
from firexapp.submit.console import setup_console_logging
from firexapp.application import import_microservices, get_app_tasks, get_app_task
from firexapp.engine.celery import app
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.submit.shutdown import launch_background_shutdown

add_hostname_to_log_records()
logger = setup_console_logging(__name__)


SUBMISSION_FILE_REGISTRY_KEY = 'firex_submission'
FileRegistry().register_file(SUBMISSION_FILE_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'submission.txt'))

ENVIRON_FILE_REGISTRY_KEY = 'env'
FileRegistry().register_file(ENVIRON_FILE_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'environ.json'))


class SubmitBaseApp:
    SUBMISSION_LOGGING_FORMATTER = '[%(asctime)s %(levelname)s] %(message)s'
    DEFAULT_MICROSERVICE = None
    PRIMARY_WORKER_NAME = "mc"

    def __init__(self, submission_tmp_file=None):
        self.submission_tmp_file = submission_tmp_file
        self.uid = None
        self.broker = None
        self.celery_manager = None
        self.is_sync = None
        self.enabled_tracking_services = None

    def init_file_logging(self):
        os.umask(0)
        if self.submission_tmp_file:
            submission_log_handler = logging.FileHandler(filename=self.submission_tmp_file)
            submission_log_handler.setFormatter(logging.Formatter(fmt=self.SUBMISSION_LOGGING_FORMATTER,
                                                                  datefmt="%Y-%m-%d %H:%M:%S"))
            submission_log_handler.setLevel(logging.NOTSET)
            root_logger = logging.getLogger()
            root_logger.addHandler(submission_log_handler)
        self.log_preamble()

    def copy_submission_log(self):
        if self.submission_tmp_file and os.path.isfile(self.submission_tmp_file) and self.uid:
            copyfile(self.submission_tmp_file, FileRegistry().get_file(SUBMISSION_FILE_REGISTRY_KEY, self.uid.logs_dir))

    def log_preamble(self):
        """Overridable method to allow a firex application to log on startup"""
        pass

    def create_submit_parser(self, sub_parser):
        submit_parser = sub_parser.add_parser("submit",
                                              help="This tool invokes a fireX run and is the most common use of "
                                                   "firex. Check out our documentation for common usage patterns",
                                              parents=[plugin_support_parser],
                                              formatter_class=argparse.RawDescriptionHelpFormatter)
        submit_parser.add_argument('--chain', '-chain', help='A comma delimited list of microservices to run',
                                   default=self.DEFAULT_MICROSERVICE)
        submit_parser.add_argument('--sync', '-sync', help='Hold console until run completes', nargs='?', const=True,
                                   default=False)
        submit_parser.add_argument('--disable_tracking_services',
                                   help='A comma delimited list of tracking services to disable.', default='')
        submit_parser.add_argument('--logs_link',
                                   help="Create a symlink back the root of the run's logs directory")
        submit_parser.add_argument('--soft_time_limit', help="Task default soft_time_limit", type=int)
        submit_parser.set_defaults(func=self.run_submit)

        for service in get_tracking_services():
            service.extra_cli_arguments(submit_parser)
        return submit_parser

    def convert_chain_args(self, chain_args) -> dict:
        with self.graceful_exit_on_failure("The arguments you provided have the following errors"):
            return InputConverter.convert(**chain_args)

    def run_submit(self, args, others):
        self.is_sync = args.sync
        try:
            self.init_file_logging()
            return self.submit(args, others)
        finally:
            self.copy_submission_log()

    @staticmethod
    def error_banner(err_msg, banner_title='ERROR', logf=logger.error):
        err_msg = str(err_msg)

        banner_title = ' %s ' % banner_title

        sep_len = len(banner_title) + 6
        err_msg = err_msg.split('\n')
        for l in err_msg:
            if len(l) > sep_len:
                sep_len = len(l)

        top_sep_len = int((sep_len - len(banner_title) + 1) / 2)
        top_banner = '*' * top_sep_len + banner_title + '*' * top_sep_len

        logf('')
        logf(top_banner)
        for l in err_msg:
            logf(l)
        logf('*'*len(top_banner))

    def create_logs_link(self, logs_link):
        try:
            os.symlink(self.uid.logs_dir, logs_link)
        except FileExistsError:
            logger.warning(f'Target {logs_link} exists, removing it first.')
            os.remove(logs_link)
            os.symlink(self.uid.logs_dir, logs_link)
        logger.debug('Symbolic link created: %s -> %s' % (self.uid.logs_dir, logs_link))

    def submit(self, args, others):
        chain_args = self.process_other_chain_args(args, others)

        uid = Uid()
        self.uid = uid
        chain_args['uid'] = uid
        logger.info("FireX ID: %s", uid)
        logger.info('Logs: %s', uid.logs_dir)

        if args.logs_link:
            self.create_logs_link(args.logs_link)

        # Create an env file for debugging
        with open(FileRegistry().get_file(ENVIRON_FILE_REGISTRY_KEY, self.uid.logs_dir), 'w') as f:
            json.dump(dict(os.environ), fp=f, skipkeys=True, sort_keys=True, indent=4)

        chain_args = self.convert_chain_args(chain_args)

        chain_args = self.start_engine(args=args, chain_args=chain_args, uid=uid)

        # Execute chain
        try:
            root_task_name = app.conf.get("root_task")
            if root_task_name is None:
                raise NotRegistered("No root task configured")
            root_task = get_app_task(root_task_name)
        except NotRegistered as e:
            logger.error(e)
            self.main_error_exit_handler(reason=str(e))
            sys.exit(-1)
        self.wait_tracking_services_task_ready()
        chain_result = root_task.s(chain=args.chain, submit_app=self, sync=args.sync, **chain_args).delay()

        self.copy_submission_log()

        if args.sync:
            logger.info("Waiting for chain to complete...")
            try:
                wait_on_async_results(chain_result)
                self.check_for_failures(chain_result, chain_args)
            except (ChainRevokedException, ChainInterruptedException) as e:
                self.check_for_failures(chain_result, chain_args)
                self.main_error_exit_handler(chain_details=(chain_result, chain_args),
                                             reason=f"Sync run: completed unsuccessfully ({e})")
                sys.exit(-1)
            else:
                logger.info("All tasks succeeded")
                self.copy_submission_log()
                self.self_destruct(chain_details=(chain_result, chain_args),
                                   reason="Sync run: completed successfully")

        self.wait_tracking_services_release_console_ready()

    @staticmethod
    def get_all_failures(chain_result):
        return find_all_unsuccessful(chain_result, ignore_non_ready=True)

    def check_for_failures(self, chain_result, chain_args):
        failures = self.get_all_failures(chain_result)
        if failures:
            logger.error("Failures occurred in the following tasks:")
            failures = sorted(failures.values())
            for failure in failures:
                logger.error(failure)
            self.main_error_exit_handler(chain_details=(chain_result, chain_args),
                                         reason=f'Tasks failed: {failures}.')
            sys.exit(-1)

    def set_broker_in_app(self):
        from firexapp.engine.celery import app
        broker_url = self.broker.get_url()
        BrokerFactory.set_broker_env(broker_url)

        app.conf.result_backend = broker_url
        app.conf.broker_url = broker_url

    def start_engine(self, args, chain_args, uid)->{}:
        # Start Broker
        self.start_broker(args=args)
        self.set_broker_in_app()

        try:
            # start backend
            app.backend.set('uid', str(uid))
            app.backend.set('logs_dir', uid.logs_dir)

            # IMPORT ALL THE MICROSERVICES
            # ONLY AFTER BROKER HAD STARTED
            all_tasks = import_microservices(chain_args.get("plugins", args.plugins))
        except FileNotFoundError as e:
            logger.error("\nError: FireX run failed. File %s is not found." % e)
            self.main_error_exit_handler(reason=str(e))
            sys.exit(-1)
        except Exception as e:
            logger.error("An error occurred while loading modules")
            logger.exception(e)
            self.main_error_exit_handler(reason=str(e))
            sys.exit(-1)

        # Post import converters
        chain_args = self.convert_chain_args(chain_args)

        # check argument applicability to detect useless input arguments
        if not self.validate_argument_applicability(chain_args, args, all_tasks):
            self.main_error_exit_handler(reason="Inapplicable arguments.")
            sys.exit(-1)

        # locate task objects
        try:
            app_tasks = get_app_tasks(args.chain)
        except NotRegistered as e:
            reason = "Could not find task %s" % str(e)
            logger.error(reason)
            self.main_error_exit_handler(reason=reason)
            sys.exit(-1)

        # validate that all necessary chain args were provided
        c = InjectArgs(**chain_args)
        for t in app_tasks:
            c |= t.s()
        try:
            verify_chain_arguments(c)
        except InvalidChainArgsException as e:
            self.error_banner(e)
            self.main_error_exit_handler(reason=str(e))
            sys.exit(-1)

        with self.graceful_exit_on_failure("Failed to start tracking service"):
            # Start any tracking services to monitor, track, and present the state of the run
            chain_args.update(self.start_tracking_services(args, **chain_args))

        # Start Celery
        with self.graceful_exit_on_failure("Unable to start Celery."):
            self.start_celery(args, chain_args.get("plugins", args.plugins))
        return chain_args

    def start_celery(self, args, plugins):
        from firexapp.celery_manager import CeleryManager
        import multiprocessing
        celery_manager = CeleryManager(logs_dir=self.uid.logs_dir, plugins=plugins)
        celery_manager.start(workername=self.PRIMARY_WORKER_NAME, wait=True, concurrency=multiprocessing.cpu_count()*4,
                             soft_time_limit=args.soft_time_limit)
        self.celery_manager = celery_manager

    def process_other_chain_args(self, args, other_args)-> {}:
        try:
            chain_args = get_chain_args(other_args)
        except ChainArgException as e:
            logger.error(str(e))
            logger.error('Aborting...')
            sys.exit(-1)

        # 'plugins' is a necessary element of the chain args, so that they can be handled by converters
        if args.plugins:
            chain_args['plugins'] = args.plugins

        if args.soft_time_limit:
            chain_args['soft_time_limit'] = args.soft_time_limit

        return chain_args

    def start_broker(self, args):
        from firexapp.broker_manager.broker_factory import BrokerFactory
        self.broker = BrokerFactory.create_new_broker_manager(logs_dir=self.uid.logs_dir)
        self.broker.start()

    def start_tracking_services(self, args, **chain_args)->{}:
        assert self.enabled_tracking_services is None, "Cannot start tracking services twice."
        self.enabled_tracking_services = []
        services = get_tracking_services()
        if services:
            logger.debug("Tracking services:")
            disabled_service_names = args.disable_tracking_services.split(',')
            for service in services:
                service_name = get_service_name(service)
                service_enabled = service_name not in disabled_service_names
                disabled_str = '' if service_enabled else ' (disabled)'
                logger.debug("\t%s%s" % (service_name, disabled_str))
                if service_enabled:
                    self.enabled_tracking_services.append(service)

        additional_chain_args = {}
        for service in self.enabled_tracking_services:
            extra = service.start(args, **chain_args)
            if extra:
                additional_chain_args.update(extra)
        return additional_chain_args

    def wait_tracking_services_pred(self, service_predicate, description, timeout)->None:
        services_by_name = {get_service_name(s): s for s in self.enabled_tracking_services}
        not_passed_pred_services = list(services_by_name.keys())
        start_wait_time = time.time()
        timeout_max = start_wait_time + timeout

        while not_passed_pred_services and time.time() < timeout_max:
            for service_name in not_passed_pred_services:
                if service_predicate(services_by_name[service_name]):
                    # Service has passed the predicate, remove it from the list of not passed services.
                    not_passed_pred_services = [n for n in not_passed_pred_services if service_name != n]
                    if not not_passed_pred_services:
                        logger.debug(f"Last tracking service up (long pole) is: {service_name}")
            if not_passed_pred_services:
                time.sleep(0.1)

        if not_passed_pred_services:
            logger.warning("The following services are still not %s after %s secs:" % (description, timeout))
            for s in not_passed_pred_services:
                launch_file = getattr(services_by_name[s], 'stdout_file', None)
                msg = f'{s}: see {launch_file}' if launch_file else s
                logger.warning('\t' + msg)
        else:
            wait_duration = time.time() - start_wait_time
            logger.debug("Waited %.1f secs for tracking services to be %s." % (wait_duration, description))

    def wait_tracking_services_task_ready(self, timeout=5)->None:
        self.wait_tracking_services_pred(lambda s: s.ready_for_tasks(), 'ready for tasks', timeout)

    def wait_tracking_services_release_console_ready(self, timeout=5)->None:
        self.wait_tracking_services_pred(lambda s: s.ready_release_console(), 'ready to release console', timeout)

    def main_error_exit_handler(self, chain_details=None, reason=None):
        mssg = 'Aborting FireX submission...'
        if reason:
            mssg += '\n' + str(reason)
        logger.error(mssg)
        if self.broker:
            self.self_destruct(chain_details=chain_details, reason=reason)
        if self.uid:
            self.copy_submission_log()

    def self_destruct(self, chain_details=None, reason=None):
        if chain_details:
            chain_result, chain_args = chain_details
            try:
                logger.debug("Generating reports")
                from firexapp.submit.reporting import ReportersRegistry
                ReportersRegistry.post_run_report(results=chain_result, kwargs=chain_args)
                logger.debug('Reports successfully generated')
            except Exception:
                # Under no circumstances should report generation prevent celery and broker cleanup
                logger.error('Error in generating reports', exc_info=True)
            finally:
                if chain_result:
                    disable_async_result(chain_result)

        logger.debug("Running FireX self destruct")
        launch_background_shutdown(self.uid.logs_dir, reason)

    @classmethod
    def validate_argument_applicability(cls, chain_args, args, all_tasks):
        if isinstance(args, argparse.Namespace):
            args = vars(args)
        if isinstance(args, dict):
            args = list(args.keys())

        unused_chain_args, matches = find_unused_arguments(chain_args=chain_args,
                                                           ignore_list=args,
                                                           all_tasks=all_tasks)
        if not unused_chain_args:
            # everything is used. Good job!
            return True

        logger.error("Invalid arguments provided. The following arguments are not used by any microservices:")
        for arg in unused_chain_args:
            if arg in matches:
                logger.error("--" + arg + " (Did you mean '%s'?)" % matches[arg])
            else:
                logger.error("--" + arg)
        return False

    @contextmanager
    def graceful_exit_on_failure(self, failure_caption: str):
        try:
            yield
        except Exception as e:
            logger.debug(traceback.format_exc())
            self.error_banner(e, banner_title=failure_caption)
            self.main_error_exit_handler(reason=failure_caption)
            sys.exit(-1)


def get_firex_id_from_output(cmd_output: str)->str:
    for line in cmd_output.splitlines():
        match = re.match('.*FireX ID: (.*)', line)
        if match:
            return match.group(1)


def get_log_dir_from_output(cmd_output: str)->str:
    if not cmd_output:
        return ""

    lines = cmd_output.split("\n")
    log_dir_key = "Logs: "
    try:
        logs_lines = [line.split(log_dir_key)[1] for line in lines if log_dir_key in line]
        log_dir_line = logs_lines[-1]
        return log_dir_line.strip()
    except IndexError:
        return ""


# noinspection PyUnusedLocal
@celeryd_init.connect()
def add_uid_to_conf(conf=None, **kwargs):
    conf.uid = app.backend.get('uid').decode()


@worker_ready.connect()
def celery_worker_ready(sender, **_kwargs):
    queue_names = [queue.name for queue in sender.task_consumer.queues]
    if queue_names:
        mark_queues_ready(*queue_names)
