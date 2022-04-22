import os
from tempfile import NamedTemporaryFile
import json

from firexkit.argument_conversion import SingleArgDecorator
from firexapp.engine.celery import app
from firexapp.fileregistry import FileRegistry
from firexapp.submit.arguments import InputConverter
from firexapp.submit.submit import SUBMISSION_FILE_REGISTRY_KEY, get_log_dir_from_output
from firexapp.submit.uid import Uid
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run, assert_is_good_run
from firexapp.tasks.example import nop, sleep
from firexapp.application import JSON_ARGS_PATH_ARG_NAME


@InputConverter.register("convert_booleans")
@SingleArgDecorator("barf")
def do_i_barf(arg_value):
    if arg_value:
        raise Exception("Barf")


def get_submission_file(logs_dir: str):
    submission_file = FileRegistry().get_file(SUBMISSION_FILE_REGISTRY_KEY, logs_dir)
    assert os.path.isfile(submission_file), "submission file missing not there"
    return submission_file


class SubmitConvertFailureStillHasLogs(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "nop", "--barf", "True"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "barf: Barf" in cmd_err, "Error in converter did not show up"
        submission_file = get_submission_file(get_log_dir_from_output(cmd_output))

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


@app.task(bind=True)
def write_a_test_file(self, uid: Uid):
    test_file_path = os.path.join(uid.logs_dir, "success")
    with open(test_file_path, "w+") as f:
        f.write("success")
    self.enqueue_child(nop.s())
    self.enqueue_child(sleep.s(sleep=0.001))
    self.enqueue_child(sleep.s())


class SubmitHighRunnerCase(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "write_a_test_file"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        test_file_path = os.path.join(self.run_data.logs_path, "success")
        assert os.path.isfile(os.path.join(test_file_path)), "Test file was not created in the logs directory, " \
                                                             "therefor the microservice did not run"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


@app.task
def high_expectations(provided_one, provided_two, added_one, added_two):
    assert provided_one is True  # converted by a pre-converter
    assert provided_two == "provided_two"
    assert added_one == "added_one"
    assert added_two == "added_two"


@InputConverter.register
def convert_provided_and_added_one(kwargs):
    kwargs["added_one"] = "added_one"


@InputConverter.register(False)
def convert_provided_and_added_two(kwargs):
    kwargs["provided_two"] = "provided_two"
    kwargs["added_two"] = "added_two"


class ArgConverterCheck(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "high_expectations",
                "--provided_one", "True",
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
        assert 'Missing mandatory arguments:' in cmd_err
        assert 'i_need_me_some_of_this' in cmd_err
        assert 'required by "need_an_argument"' in cmd_err

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


class ArgsFromJsonFile(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        self.json_args_path = NamedTemporaryFile(mode='w', delete=False)
        json.dump(['--i_need_me_some_of_this', 'here is the arg'], self.json_args_path)
        self.json_args_path.flush()
        return ["submit",
                '--chain', 'need_an_argument',
                JSON_ARGS_PATH_ARG_NAME, self.json_args_path.name]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err

    def assert_expected_return_code(self, ret_value):
        os.unlink(self.json_args_path.name)
        assert_is_good_run(ret_value)
