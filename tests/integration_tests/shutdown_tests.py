import os
import abc
from glob import glob
from pathlib import Path
from psutil import Process
import signal

from firexapp.engine.celery import app
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.submit.arguments import InputConverter
from firexapp.submit.reporting import ReportGenerator, report
from firexapp.submit.tracking_service import TrackingService, get_tracking_services
import firexapp.submit.tracking_service
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run
from firexapp.celery_manager import CeleryManager


def get_broker_url_from_output(cmd_output):
    lines = cmd_output.split("\n")

    # export BROKER=redis://ott-ads-033:47934/0
    export_broker_tag = "export %s=" % BrokerFactory.broker_env_variable
    export_line = [line for line in lines if export_broker_tag in line][0]
    return export_line.split(export_broker_tag)[-1]


def get_leaked_broker_process(cmd_output):
    broker_url = get_broker_url_from_output(cmd_output)
    assert broker_url, "No broker was exported"

    old_broker_env_variable = os.environ.get(BrokerFactory.broker_env_variable)
    try:
        # temporarily set the broker_url to get the same broker manager as the run
        os.environ[BrokerFactory.broker_env_variable] = broker_url
        broker_manager = BrokerFactory.get_broker_manager()
        assert broker_manager.get_url() == broker_url
        if broker_manager.is_alive():
            return broker_manager
        return None
    finally:
        # don't disrupt the environment
        if old_broker_env_variable:
            os.environ[BrokerFactory.broker_env_variable] = old_broker_env_variable


class NoBrokerLeakBase(FlowTestConfiguration):
    __metaclass__ = abc.ABCMeta

    def initial_firex_options(self) -> list:
        raise NotImplementedError("This is a base class")

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        broker = get_leaked_broker_process(cmd_output)
        assert not broker, "We are leaking a broker: " + str(broker)
        assert self.expected_error() in cmd_err, "Different error expected"

    @abc.abstractmethod
    def expected_error(self):
        pass

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


class NoBrokerLeakOnBadTask(NoBrokerLeakBase):
    """ Broker is started but not celery. Make sure the broker is release"""

    def expected_error(self):
        return "Could not find task 'not_exist'"

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "not_exist"]


@app.task
def failure():
    raise Exception("This exception is part of the test")


class NoBrokerLeakOnTaskFailure(NoBrokerLeakBase):
    """ Broker and celery are started. Make sure the broker is release """
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "failure"]

    def expected_error(self):
        return "Failures occurred in the following tasks"


@InputConverter.register
def get_main_pid(kwargs):
    if "pid" in kwargs:
        return {"pid": os.getpid()}


@app.task
def kill_firexapp_main(pid):
    os.kill(int(pid), signal.SIGHUP)


class NoBrokerLeakOnCtrlC(NoBrokerLeakBase):
    """ Have the microservice send a sigint to the main """
    timeout = 90

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "kill_firexapp_main", "--pid", "tbd"]

    def expected_error(self):
        return "Exiting due to signal SIGHUP"


class ExplodingReport(ReportGenerator):
    def __init__(self):
        self.primed = False

    def add_entry(self, key_name, value, priority, formatters, task_name=None, **extra):
        if task_name.split(".")[-1] == the_bomb.__name__:
            self.primed = True
            raise Exception("raised by add_entry(). Someone set us up the bomb")

    def post_run_report(self, **kwargs):
        if self.primed:
            raise Exception("raised by post_run_report(). All your bases are belong to us")


@report(key_name=None, priority=1)
@app.task
def the_bomb():
    pass


class NoBrokerLeakOnBadReportGenerator(NoBrokerLeakBase):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "the_bomb"]

    def expected_error(self):
        return ""

    def assert_expected_return_code(self, ret_value):
        pass  # it's better if the test fails on the redis leak


@app.task
def fail_service_task():
    pass


class FailingService(TrackingService):
    def start(self, args, **chain_args)->{}:
        if args.chain == "fail_service_task":
            raise Exception("Failed to start service")


existing_services = get_tracking_services()
firexapp.submit.tracking_service._services = tuple(list(existing_services) + [FailingService()])


class NoBrokerLeakOnFailedService(NoBrokerLeakBase):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "fail_service_task"]

    def expected_error(self):
        return "Failed to start service"

    def assert_expected_return_code(self, ret_value):
        pass  # it's better if the test fails on the redis leak


class NoBrokerLeakOnBadPlugin(NoBrokerLeakBase):
    def initial_firex_options(self) -> list:
        bad_plugin = os.path.join(os.path.dirname(__file__), "data", "shutdown", "bad_module.py")
        return ["submit", "--chain", "nop", "--plugin", bad_plugin]

    def expected_error(self):
        return "An error occurred while loading modules"

    def assert_expected_return_code(self, ret_value):
        pass  # it's better if the test fails on the redis leak


class NoBrokerLeakOnCeleryFailure(NoBrokerLeakBase):
    def initial_firex_options(self) -> list:
        bad_plugin = os.path.join(os.path.dirname(__file__), "data", "shutdown", "fail_celery_start.py")
        return ["submit", "--chain", "nop", "--plugin", bad_plugin]

    def expected_error(self):
        return "Unable to start Celery."

    def assert_expected_return_code(self, ret_value):
        pass  # it's better if the test fails on the redis leak


@app.task
def revoke_root_task():
    from firexapp.tasks.core_tasks import get_configured_root_task
    root = get_configured_root_task()
    from firexkit.inspect import get_active
    active = get_active()
    if active:
        for host in active.values():
            for task in host:
                if root.__name__ in task['name']:
                    app.control.revoke(task_id=task["id"], terminate=True)

    # sleep till shutdown revokes us
    from time import sleep
    sleep(5)


class NoBrokerLeakOnRootRevoke(NoBrokerLeakBase):
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "revoke_root_task"]

    def expected_error(self):
        return "Aborting FireX submission..."

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


@app.task
def terminate_celery(uid):
    pid_file = glob(os.path.join(uid.logs_dir, 'debug', 'celery', 'pids', '*.pid'))[0]
    Process(int(Path(pid_file).read_text())).kill()


class NoBrokerLeakOnCeleryTerminated(NoBrokerLeakBase):
    # TODO: note this test is slow because it deliberately waits 15s for normal celery shutdown.
    # It might be worth making the celery shutdown timeout a parameter.

    # It isn't completely clear why, but coverage causes the CI to hang after this test has completed.
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "terminate_celery"]

    def assert_expected_return_code(self, ret_value):
        pass  # it's better if the test fails on the redis leak

    def expected_error(self):
        return ""

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        super().assert_expected_firex_output(cmd_output, cmd_err)
        logs_dir = get_log_dir_from_output(cmd_output)

        existing_procs = []
        celery_pids_dir = CeleryManager(logs_dir=logs_dir).celery_pids_dir
        for f in os.listdir(celery_pids_dir):
            existing_procs += CeleryManager.find_procs(os.path.join(celery_pids_dir, f))

        assert not existing_procs, "Expected no remaining celery processes, found: %s" % existing_procs
