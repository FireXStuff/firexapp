
from celery import current_app
from firexkit.task import FireXTask
from firexapp.testing.config_base import FlowTestConfiguration


# noinspection PyUnusedLocal
@current_app.task(base=FireXTask)
def a_service_to_test(use_this_arg):
    pass


class InfoFindsMicroservice(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["info", "a_service_to_test"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Short Name: a_service_to_test" in cmd_output, "Test info not provided"
        assert "Full Name: info_tests.a_service_to_test" in cmd_output, "Test info not provided"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert ret_value is 0, "Test expects a CLEAN run, but returned %s. " \
                               "Check the err output to see what went wrong." % str(ret_value)


class InfoFindsArgument(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["info", "use_this_arg"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Argument name: use_this_arg" in cmd_output, "Test info not provided"
        assert "info_tests.a_service_to_test" in cmd_output, "Test info not provided"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert ret_value is 0, "Test expects a CLEAN run, but returned %s. " \
                               "Check the err output to see what went wrong." % str(ret_value)
