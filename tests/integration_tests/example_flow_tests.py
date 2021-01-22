#
# FireX provides a flow_test harness for running integration tests of entire FireX runs.
# The GreetTest example is overkill, but should give you an idea of how to write flow tests. Execute the following
# from the root of a firex install:
#   flow_tests/flow_test_infra.py --tests plugins/firex_programming_guide.py
#
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.reporters.json_reporter import get_completion_report_data
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run, assert_is_bad_run


class GreetTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "greet", "--name", "John"]

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "no errors expected"

        logs_dir = get_log_dir_from_output(cmd_output)
        completion_data = get_completion_report_data(logs_dir)

        assert completion_data['completed']
