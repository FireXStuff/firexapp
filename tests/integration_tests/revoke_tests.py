import os
import glob

from time import sleep

from firex_keeper import task_query

from firexapp.engine.celery import app
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run

@app.task()
def revoke(root_uuid):
    # revoking root should revoke this task too.
    app.control.revoke(root_uuid, terminate=True)
    sleep(120)


@app.task(bind=True)
def revoke_root_via_child(self):
    root_uuid = self.request.parent_id
    self.enqueue_child(revoke.s(root_uuid=root_uuid), block=True)

class RevokeOnShutdown(FlowTestConfiguration):
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "revoke_root_via_child"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = self.run_data.logs_path

        # We use files to indicate some failures because we can't log
        # anything in our logs during shutdown
        failure_files = glob.glob(os.path.join(logs_dir, 'test_fail_*'))
        failures_text = ''
        for fail in failure_files:
            with open(fail,'r') as f:
                failures_text += fail + ' : ' + f.read() + '\n'
        assert not failures_text, failures_text

        keeper_complete = task_query.wait_on_keeper_complete(logs_dir, timeout=60)
        assert keeper_complete, "Keeper database is not complete."

        revoked_task_count = len(task_query.revoked_tasks(logs_dir))
        assert revoked_task_count == 3, f"Not all 3 tasks were revoked, was: {revoked_task_count}"

        assert self.run_data.revoked, "run.json should indicate revoked."

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)
