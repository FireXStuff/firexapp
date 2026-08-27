import os

from firexapp.engine.celery import app
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run, assert_is_bad_run
from firexapp.events.model import RunStates
from firexapp.common import wait_until
from firexkit.chain import returns
from firexkit.task import FireXTask

from firex_keeper import task_query
from firex_keeper.persist import get_db_manager, task_by_uuid_exp


@app.task()
@returns('echoed')
def echo(arg_echo):
    return arg_echo


class KeepNoopData(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "echo", '--arg_echo', 'value']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        keeper_complete = task_query.wait_on_keeper_complete(logs_dir)
        assert keeper_complete, "Keeper database is not complete."

        tasks = task_query.tasks_by_name(logs_dir, 'echo')
        assert tasks[0].name == 'echo'
        assert tasks[0].state == RunStates.SUCCEEDED.value

        firex_id = os.path.basename(logs_dir)
        with get_db_manager(logs_dir) as db_manager:
            run_metadata = db_manager.query_run_metadata(firex_id)
            is_complete = wait_until(db_manager._is_keeper_complete, timeout=5, sleep_for=0.5)
            assert is_complete is True

        assert run_metadata.chain == 'echo'
        assert run_metadata.firex_id == firex_id
        assert run_metadata.logs_dir == logs_dir

        all_tasks = [t for t in task_query.all_tasks(logs_dir)]
        all_uuids = {t.uuid for t in all_tasks}
        assert run_metadata.root_uuid in all_uuids
        assert {firex_id} == {t.firex_id for t in all_tasks}

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


@app.task(bind=True)
def wait_before_query_on_self(self, uid):
    self_task = task_query.task_by_uuid(uid.logs_dir, self.request.id,
                                        wait_for_exp_exist=task_by_uuid_exp(self.request.id),
                                        error_on_wait_exceeded=True)
    assert self_task.uuid == self.request.id


class WaitOnSelfQueryTest(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "wait_before_query_on_self"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        pass

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class CompleteTest(FlowTestConfiguration):
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "nop"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        keeper_complete = task_query.wait_on_keeper_complete(logs_dir)
        assert keeper_complete, "Keeper database is not complete."

        all_tasks = task_query.all_tasks(logs_dir)
        assert len(all_tasks) >= 2, f"Expected more than 2 tasks, found {len(all_tasks)}"

        for t in all_tasks:
            if t.state != RunStates.SUCCEEDED.value:
                raise Exception(f"Task {t} did not succeed.")

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

@app.task(bind=True)
def FailByGrandchild(self: FireXTask):
    self.enqueue_child_and_get_results(FailByChild.s())


@app.task(bind=True)
def FailByChild(self: FireXTask):
    self.enqueue_child_and_get_results(Fail.s())


@app.task(bind=True)
def Fail(self: FireXTask):
    raise Exception("failing")


class CausingFailureTest(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "FailByGrandchild"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        keeper_complete = task_query.wait_on_keeper_complete(logs_dir)
        assert keeper_complete, "Keeper database is not complete."

        failed_by_grandchild = task_query.single_task_by_name(logs_dir, FailByGrandchild.__name__)
        failed_by_child = task_query.single_task_by_name(logs_dir, FailByChild.__name__)
        failed_by_self = task_query.single_task_by_name(logs_dir, Fail.__name__)

        assert failed_by_grandchild.exception_cause_uuid == failed_by_self.uuid, \
        f"{failed_by_grandchild} should have been failed by {failed_by_self}"

        assert failed_by_child.exception_cause_uuid == failed_by_self.uuid, \
        f"{failed_by_child} should have been failed by {failed_by_self}"

        failed_ancestors = task_query.failed_by_tasks(logs_dir, failed_by_self.uuid)
        assert len(failed_ancestors) == 2, f'Expected 2 failed ancestors'
        expected_failed_ancestor_uuids = {failed_by_child.uuid, failed_by_grandchild.uuid}
        actual_failed_ancestor_uuids = {t.uuid for t in failed_ancestors}
        assert expected_failed_ancestor_uuids == actual_failed_ancestor_uuids, \
            f"{expected_failed_ancestor_uuids} != {actual_failed_ancestor_uuids}"


    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)
