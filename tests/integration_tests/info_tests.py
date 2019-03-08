
from celery import current_app
from firexkit.chain import returns
from firexkit.task import FireXTask
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run, assert_is_good_run


# noinspection PyUnusedLocal
@current_app.task(base=FireXTask, bind=True)
@returns("something_good")
def a_service_to_test(self, use_this_arg, and_maybe_this_one=None):
    # this microservice has a bit of everything
    pass


# we could use triple quotes here, but I was the string to match without copy/paste
doc = "this is the documentation"
a_service_to_test.__doc__ = doc


class InfoFindsMicroservice(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["info", "a_service_to_test"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Short Name: a_service_to_test" in cmd_output, "Test info not provided"
        assert "Full Name: %s.a_service_to_test" % self.__module__ in cmd_output, "Test info not provided"
        assert doc in cmd_output, "Documentation was not picked up"
        assert "use_this_arg" in cmd_output, "positional argument was not included in output"
        assert "and_maybe_this_one" in cmd_output, "optional argument was not included in output"
        assert "self" not in cmd_output, "self should be excluded from arguments"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


# noinspection PyUnusedLocal
@current_app.task(base=FireXTask)
def a_second_service_to_test():
    pass


class InfoFindsFullNameMicroservice(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["info", self.__module__ + ".a_second_service_to_test"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Short Name: a_second_service_to_test" in cmd_output, "Test info not provided"
        assert "Full Name: %s.a_second_service_to_test" % self.__module__ in cmd_output, "Test info not provided"
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
        return ["info", "does.hot.exist"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Microservice does.hot.exist was not found!" in cmd_err, "Error not provided"

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


# noinspection PyUnusedLocal
@current_app.task(base=FireXTask)
def second_to_use_arg(uid, use_this_arg):  # this is the same arg name as the microservice at the top of this module
    pass


@current_app.task()
def support_none_firex_tasks():
    pass


class ListArguments(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["list", "--arguments"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "The following arguments are used by microservices" in cmd_output, "List header not there"
        assert "use_this_arg" in cmd_output, "Test microservice not listed"
        assert not cmd_err, "No errors expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

