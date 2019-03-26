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


class SubmitConvertFailureStillHasLogs(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["submit", "--barf", "True"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert "barf: Barf" in cmd_err, "Error in converter did not show up"
        logs_dir = get_log_dir_from_output(cmd_output)
        submission_file = FileRegistry().get_file(SUBMISSION_FILE_REGISTRY_KEY, logs_dir)
        assert os.path.isfile(submission_file), "submission file missing not there"

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
