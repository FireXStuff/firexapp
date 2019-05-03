import os
from time import sleep

from celery.signals import task_revoked
from firexapp.engine.celery import app
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run


@app.task
def to_be_revoked_on_finish():
    sleep(120)


@app.task
def schedule_and_continue():
    to_be_revoked_on_finish.s().apply_async(add_to_parent=False)


# noinspection PyUnusedLocal
@task_revoked.connect(sender=to_be_revoked_on_finish)
def write_success_file(sender, request, terminated, signum, expired, **kwargs):
    # noinspection PyProtectedMember
    logs_dir = app.backend.get("logs_dir").decode()
    with open(os.path.join(logs_dir, "success"), "w+") as f:
        pass


class RevokeOnShutdown(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "schedule_and_continue"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        assert os.path.isfile(os.path.join(logs_dir, "success")), "Success file was not generated. Task was not revoked"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
