import os
import abc
import signal

from firexapp.engine.celery import app
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.submit.arguments import InputConverter
from firexapp.submit.reporting import ReportGenerator, report
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run


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
