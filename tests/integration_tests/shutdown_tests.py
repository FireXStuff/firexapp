import os
import abc
import signal
from time import sleep

from celery.utils.log import get_task_logger

from firexapp.engine.celery import app
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.broker_manager.redis_manager import RedisManager
from firexapp.fileregistry import FileRegistry
from firexapp.submit.arguments import InputConverter
from firexapp.submit.reporting import ReportGenerator, report
from firexapp.submit.tracking_service import TrackingService, get_tracking_services
import firexapp.submit.tracking_service
from firexapp.submit.submit import get_log_dir_from_output, RUN_COMPLETE_REGISTRY_KEY
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run, assert_is_good_run
from firexapp.celery_manager import CeleryManager
from firexapp.common import wait_until
from firexapp.tasks.root_tasks import get_configured_root_task
from firexapp.submit.shutdown import launch_background_shutdown


from psutil import Process

logger = get_task_logger(__name__)


test_data_dir = os.path.join(os.path.dirname(__file__), "data")

def get_broker(cmd_output):
    logs_dir = get_log_dir_from_output(cmd_output)
    file_exists = wait_until(lambda: os.path.exists(RedisManager.get_metadata_file(logs_dir)),
                             timeout=10,
                             sleep_for=0.5)
    assert file_exists, "No broker metadata file."
    broker = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
    return broker


def wait_until_broker_not_alive(broker):
    return wait_until(lambda b: not b.is_alive(), 60, 0.5, broker)


class NoBrokerLeakBase(FlowTestConfiguration):
    __metaclass__ = abc.ABCMeta

    def initial_firex_options(self) -> list:
        raise NotImplementedError("This is a base class")

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        broker = get_broker(cmd_output)
        broker_not_alive = wait_until_broker_not_alive(broker)
        assert broker_not_alive, "We are leaking a broker: " + str(broker)
        expected_error = self.expected_error()
        assert expected_error in cmd_err, f"Different error expected; expected '{expected_error}', got '{cmd_err}'"

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
        return "The following microservices failed"


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

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        super().assert_expected_firex_output(cmd_output, cmd_err)

        assert self.completed_run.completed, 'expected run.json to indicate completed'
        assert self.completed_run.revoked, 'expected run.json to indicate revoked'
        assert isinstance(self.completed_run.inputs['pid'], int), f'input not captured properly: {self.completed_run.inputs["pid"]}'


    def expected_error(self):
        return "Exiting due to signal SIGHUP"


class ExplodingReport(ReportGenerator):
    def __init__(self):
        self.primed = False

    def load_data(self, key_name, value, loaders, **extra):
        pass

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
    def start(self, args, **chain_args) -> {}:
        if args.chain == "fail_service_task":
            raise Exception("Failed to start service")


existing_services = get_tracking_services()
firexapp.submit.tracking_service._services = tuple(list(existing_services) + [FailingService()])


class NoBrokerLeakOnFailedService(NoBrokerLeakBase):
    def initial_firex_options(self) -> list:
        install_configs = os.path.join(test_data_dir, 'tracking_services', 'install-configs-failing-launcher.json')
        return ["submit", "--chain", "fail_service_task",
                "--install_configs", install_configs]

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
    from firexapp.tasks.root_tasks import get_configured_root_task
    root = get_configured_root_task()
    from firexkit.inspect import get_active
    active = get_active()
    if active:
        for host in active.values():
            for task in host:
                if root.__name__ in task['name']:
                    logger.info("Revoking %s" % task['name'])
                    app.control.revoke(task_id=task["id"], terminate=True)

    # sleep till shutdown revokes us. We should not end up sleeping for this long.
    sleep(100)


class NoBrokerLeakOnRootRevoke(NoBrokerLeakBase):
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "revoke_root_task"]

    def expected_error(self):
        return f"The chain has been interrupted by the revocation of microservice {get_configured_root_task().name}"

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


class AsyncNoBrokerLeakOnRootRevoke(NoBrokerLeakBase):
    no_coverage = True
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "revoke_root_task"]

    def expected_error(self):
        return ""

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


@app.task
def terminate_celery(uid):
    worker_name = app.conf.primary_worker_name
    pid = CeleryManager.get_pid(uid.logs_dir, worker_name)
    logger.info(f'Killing pid {pid} for {worker_name}')
    Process(pid).kill()


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
        celery_pids_dir = CeleryManager(logs_dir=logs_dir, broker=get_broker(cmd_output)).celery_pids_dir
        for f in os.listdir(celery_pids_dir):
            existing_procs += CeleryManager.find_procs(os.path.join(celery_pids_dir, f))

        assert not existing_procs, "Expected no remaining celery processes, found: %s" % existing_procs

        completion_file = FileRegistry().get_file(RUN_COMPLETE_REGISTRY_KEY, logs_dir)
        assert os.path.exists(completion_file), f'RUN_COMPLETED is expected to be found in {completion_file}'


class AsyncNoBrokerLeakOnCeleryTerminated(NoBrokerLeakOnCeleryTerminated):
    # TODO: note this test is slow because it deliberately waits 15s for normal celery shutdown.
    # It might be worth making the celery shutdown timeout a parameter.

    # It isn't completely clear why, but coverage causes the CI to hang after this test has completed.
    no_coverage = True
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "terminate_celery"]

    def assert_expected_return_code(self, ret_value):
        pass  # it's better if the test fails on the redis leak

    def expected_error(self):
        return ""


class ShutdownDetachedFromParentProcess(NoBrokerLeakOnCeleryTerminated):
    # It isn't completely clear why, but coverage causes the CI to hang after this test has completed.
    no_coverage = True
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "Sleep", '--sleep', '20']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        shutdown_pid = launch_background_shutdown(
            logs_dir,
            'terminating out-of-run for integration testing',
        )
        shutdown_proc = Process(shutdown_pid)
        assert shutdown_proc.name() == 'firex_shutdown', f'Unexpected shutdown proc name: {shutdown_proc.name()}'

        child_procs = Process().children(recursive=True)
        assert shutdown_proc not in child_procs, 'Expected shutdown to not be a child of the current process'
        shutdown_proc.wait(20) # unfortunately can't confirm return code since it isn't a child.

        super().assert_expected_firex_output(cmd_output, cmd_err)
