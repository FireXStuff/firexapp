import os

from firexkit.chain import returns
from firexapp.engine.celery import app
from firexapp.submit.reporting import ReportGenerator, report
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run


class CustomTestReportGenerator(ReportGenerator):
    formatters = ("good", )  # to test the filtering functionality
    logs_dir = None

    def __init__(self):
        self.had_entries = 0

    @staticmethod
    def pre_run_report(uid, **kwarg):
        # plant a flag for pre-run-reports
        with open(os.path.join(uid.logs_dir, "initial_success"), "w+"):
            pass

    def add_entry(self, key_name, value, priority, formatters, **extra):
        assert key_name is None
        assert type(value) is dict
        assert "the_secret_to_success" in value
        assert len(formatters) == 1, "Formatters where not filtered"
        assert formatters["good"](value["the_secret_to_success"]) == "perseverance", "Formatter did not work"
        self.had_entries += 1

    def post_run_report(self, uid, **kwargs):
        if self.had_entries == 1:
            success_file = os.path.join(uid.logs_dir, "success")
            with open(success_file, 'w+'):
                # plant a flag for post-run-reports
                pass


def bad_formatter(_):
    pass


def good_formatter(x):
    return x


@report(key_name=None, priority=1,
        bad=bad_formatter,
        good=good_formatter)
@app.task
@returns("the_secret_to_success")
def secret():
    return "perseverance"


class CreateCustomReportType(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "secret"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        initial_test_file_path = os.path.join(logs_dir, "initial_success")
        assert os.path.isfile(os.path.join(initial_test_file_path)), "Initial Test file was not created in the logs " \
                                                                     "directory, therefor the report was not generated"

        test_file_path = os.path.join(logs_dir, "success")
        assert os.path.isfile(os.path.join(test_file_path)), "Test file was not created in the logs directory, " \
                                                             "therefor the report was not generated"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
