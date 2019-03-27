import os
from firexapp.engine.celery import app

from firexapp.fileregistry import FileRegistry
from firexapp.submit.arguments import InputConverter
from firexapp.submit.submit import SUBMISSION_FILE_REGISTRY_KEY
from firexapp.submit.uid import Uid
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run, assert_is_good_run
from firexkit.argument_conversion import SingleArgDecorator


@InputConverter.register("convert_booleans")
@SingleArgDecorator("barf")
def do_i_barf(arg_value):
    if arg_value:
        raise Exception("Barf")


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


def get_submission_file(cmd_output: str):
    logs_dir = get_log_dir_from_output(cmd_output)
    submission_file = FileRegistry().get_file(SUBMISSION_FILE_REGISTRY_KEY, logs_dir)
    assert os.path.isfile(submission_file), "submission file missing not there"
    return submission_file


class SubmitConvertFailureStillHasLogs(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["submit", "--barf", "True"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "barf: Barf" in cmd_err, "Error in converter did not show up"
        submission_file = get_submission_file(cmd_output)

        # open submission
        with open(submission_file) as f:
            # find Barf in submission
            for line in f:
                if "Barf" in line:
                    break
            else:
                raise AssertionError("Expected error not found in submission file")

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


@app.task
def write_a_test_file(uid: Uid):
    test_file_path = os.path.join(uid.logs_dir, "success")
    with open(test_file_path, "w+") as f:
        f.write("success")


class SubmitHighRunnerCase(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "write_a_test_file"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        test_file_path = os.path.join(logs_dir, "success")
        assert os.path.isfile(os.path.join(test_file_path)), "Test file was not created in the logs directory, " \
                                                             "therefor the microservice did not run"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


@app.task
def high_expectations(provided_one, provided_two, added_one, added_two):
    assert provided_one == "provided_one"
    assert provided_two == "provided_two"
    assert added_one == "added_one"
    assert added_two == "added_two"


@InputConverter.register
def convert_provided_and_added_one(kwargs):
    kwargs["provided_one"] = "provided_one"
    kwargs["added_one"] = "added_one"


@InputConverter.register(False)
def convert_provided_and_added_two(kwargs):
    kwargs["provided_two"] = "provided_two"
    kwargs["added_two"] = "added_two"


class ArgConverterCheck(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "high_expectations",
                "--provided_one", "needs_to_change",
                "--provided_two", "needs_to_change"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        # Any failure would not reach here
        pass

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class MisspelledChainError(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "does_not_exist"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Could not find task 'does_not_exist" in cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


@app.task
def need_an_argument(i_need_me_some_of_this):
    assert i_need_me_some_of_this


class MissingChainArgumentError(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "need_an_argument"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "Chain missing the following parameters" in cmd_err
        assert "submit_tests.need_an_argument: i_need_me_some_of_this" in cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


class InvalidArgumentError(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "need_an_argument", "--but_it_is_not_this_one", "nope"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "The following arguments are not used by any microservices" in cmd_err
        assert "--but_it_is_not_this_one" in cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


class InvalidPluginArgumentError(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "write_a_test_file", "--plugins", "does_not_exist.py"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "File does_not_exist.py is not found" in cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)
