#
# FireX provides a flow_test harness for running integration tests of entire FireX runs.
# The GreetTest example is overkill, but should give you an idea of how to write flow tests. Execute the following
# from the root of a firex install:
#   tests/integration_tests/flow_test_infra.py --tests plugins/firex_programming_guide.py
#
import os

from firexapp.submit.submit import get_log_dir_from_output
from firexapp.reporters.json_reporter import get_completion_report_data
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run, assert_is_bad_run

test_data_dir = os.path.join(os.path.dirname(__file__), "data")


class GreetTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "greet", "--name", "John"]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "no errors expected"

        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)

        assert completion_data['results']['chain_results']['greeting'] == "Hello John!"


class GreetGuestsTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "greet_guests", "--guests", "John,Mohammad"]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "no errors expected"

        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)
        assert completion_data['results']['chain_results']['guests_greeting'] == "Hello John! Hello Mohammad!"


class AmplifiedGreetGuestsTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "amplified_greet_guests", "--guests", "John,Mohammad"]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "no errors expected"

        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)
        expected_result = "Hello John! Hello Mohammad!".upper()
        actual_result = completion_data['results']['chain_results']['amplified_greeting']
        assert actual_result == expected_result, \
            "Expected '%s'  \n Received '%s'" % (expected_result, actual_result)


class GreetGuestsWithFailureTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "greet_guests", "--guests", "John,A"]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)

        expected_result = "Hello John! And apologies to those not mentioned."
        actual_result = completion_data['results']['chain_results']['guests_greeting']
        assert actual_result == expected_result, \
            "Expected '%s'  \n Received '%s'" % (expected_result, actual_result)


class GreetSpringfieldPowerPlantTest(FlowTestConfiguration):

    no_coverage = True

    def initial_firex_options(self) -> list:
        return ['submit',
                '--chain', "greet_springfield_power_plant_employees",
                "--employee_names", "Waylon Smithers,Homer Simpson",
                "--celery_concurrency", '10',
                ]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)

        expected_result = "HELLO EXECUTIVE ASSISTANT WAYLON SMITHERS! HELLO SUPERVISOR HOMER SIMPSON!"
        actual_result = completion_data['results']['chain_results']['amplified_greeting']
        assert actual_result == expected_result, \
            "Expected '%s'  \n Received '%s'" % (expected_result, actual_result)


class GreetSpringfieldPowerPlantWithPluginTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "greet_springfield_power_plant_employees",
                "--employee_names", "Homer Simpson,Waylon Smithers",
                "--plugins", os.path.join(test_data_dir, 'plugins', 'springfield_monarchy.py'),
                "--celery_concurrency", '10',
                ]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)

        expected_result = "HELLO CHANCELLOR HOMER SIMPSON! HELLO PRINCE WAYLON SMITHERS!"
        actual_result = completion_data['results']['chain_results']['amplified_greeting']
        assert actual_result == expected_result, \
            "Expected '%s'  \n Received '%s'" % (expected_result, actual_result)


class GreetLeeAndTomTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "greet_lee_and_tom"]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)
        chain_results = completion_data['results']['chain_results']

        assert chain_results['lee_greeting'] == "Hello Lee!"
        assert chain_results['tom_greeting'] == "Hello Tom!"
