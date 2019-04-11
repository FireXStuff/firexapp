import os
import abc

from firexapp.broker_manager.broker_factory import BrokerFactory
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
        assert "Could not find task 'not_exist'" in cmd_err, "Different error expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


class NoBrokerLeakOnBadTask(NoBrokerLeakBase):
    """ Broker is started but not celery. Make sure the broker is release"""
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "not_exist"]


# class NoBrokerLeakOnTaskFailure(NoBrokerLeakBase):
    """ Broker and celery are started. Make sure the broker is release """

# NoBrokerLeakOnCtrlC(NoBrokerLeakBase):
    """ Have the microservice send a sigint to the main """
