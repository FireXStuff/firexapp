import os

from firexkit.chain import returns
from firexapp.engine.celery import app
from firexapp.submit.reporting import ReportGenerator, report
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run


class CustomTestReportGenerator(ReportGenerator):
    formatters = ("good", )  # to test the filtering functionality
    logs_dir = None
    pre_run_called = False

    def __init__(self):
        self.had_entries = 0

    @staticmethod
    def pre_run_report(kwarg):
        CustomTestReportGenerator.pre_run_called = True
        CustomTestReportGenerator.logs_dir = kwarg["uid"].logs_dir

    def add_entry(self, key_name, value, priority, formatters, **extra):
        assert key_name is None
        assert type(value) is dict
        assert "the_secret_to_success" in value
        assert len(formatters) == 1, "Formatters where not filtered"
        assert formatters["good"](value["the_secret_to_success"]) == "perseverance", "Formatter did not work"
        self.had_entries += 1

    def post_run_report(self):
        # This would only work in --sync. In none-sync, the report generator instance is not the same.
        # One is on main, the other in celery
        if CustomTestReportGenerator.pre_run_called and self.had_entries == 1:
            success_file = os.path.join(CustomTestReportGenerator.logs_dir, "success")
            with open(success_file, 'w+'):
                # we'll create a black report
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
        test_file_path = os.path.join(logs_dir, "success")
        assert os.path.isfile(os.path.join(test_file_path)), "Test file was not created in the logs directory, " \
                                                             "therefor the report was not generated"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
