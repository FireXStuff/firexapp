from time import sleep

from firex_keeper import task_query

from firexapp.engine.celery import app
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run, skip_test


@app.task
def to_be_revoked_on_finish():
    sleep(120)


@app.task
def schedule_and_continue():
    to_be_revoked_on_finish.s().apply_async(add_to_parent=False)


class RevokeOnShutdown(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "schedule_and_continue"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        keeper_complete = task_query.wait_on_keeper_complete(logs_dir)
        assert keeper_complete, "Keeper database is not complete."
        assert len(task_query.revoked_tasks(logs_dir)) > 0, "Task was not revoked"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
