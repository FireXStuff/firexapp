
from celery import current_app
from firexkit.task import FireXTask
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run, assert_is_good_run


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
        assert_is_good_run(ret_value)


class InfoFindsArgument(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["info", "use_this_arg"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Argument name: use_this_arg" in cmd_output, "Test info not provided"
        assert "info_tests.a_service_to_test" in cmd_output, "Test info not provided"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class InfoCaNotFindMicroservice(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["info", "does_hot_exist"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Microservice does_hot_exist was not found!" in cmd_err, "Error not provided"

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


class ListMicroservices(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["list", "--microservices"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "The following microservices are available:" in cmd_output, "List header not there"
        assert "info_tests.a_service_to_test" in cmd_output, "Test microservice not listed"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class ListArguments(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["list", "--arguments"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "The following arguments are used by microservices" in cmd_output, "List header not there"
        assert "use_this_arg" in cmd_output, "Test microservice not listed"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

