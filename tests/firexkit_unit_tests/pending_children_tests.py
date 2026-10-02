"""
A task's base class implicitly waits for the children it left pending, bounded
by what is left of the run's own time budget.
"""

import datetime
import itertools
import time
from unittest import mock

import pytest
import pytz
from celery.states import PENDING, SUCCESS

from firexapp.engine.default_celery_config import FxEnvVars
from firexapp.submit.uid import firex_id_str
from firexkit.firex_celery import FireXCelery
from firexkit.task import FireXTask
from firexkit.testing import MockFxAsyncResult, ut_backed_celery_app


def _app_with_run_started(hours_ago: float) -> FireXCelery:
    """A throwaway app whose run started 'hours_ago', so its budget can be spent."""
    run_start = datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=hours_ago)
    return ut_backed_celery_app(
        # a run's deadline is its budget counted from the start time in its FireX ID.
        fx_env=FxEnvVars.create_no_task_exec_fx_env().model_copy(
            update={"CURRENT_RUN_FIREX_ID": firex_id_str("someuser", run_start, 1234)},
        ),
    )


def _task_leaving_a_child_pending(app: FireXCelery, child: MockFxAsyncResult):
    """A task that completes with 'child' recorded the way an enqueue leaves it."""

    @app.task(base=FireXTask)
    def parent():
        parent._update_child_state(child, FireXTask._PENDING)

    return parent


def _pending_until(deadline: float):
    """A state that stays PENDING, and complains once nothing is going to stop it.

    A wait that isn't bounded would otherwise hang the test the same way it
    hangs a run, which says far less about what went wrong.
    """

    def state() -> str:
        assert time.monotonic() < deadline, (
            "the implicit wait for children was never given up on"
        )
        return PENDING

    return state


class TestImplicitWaitForChildren:
    def test_it_gives_up_once_the_run_is_out_of_time(self, caplog):
        app = _app_with_run_started(hours_ago=5)  # well past its default budget.
        child = MockFxAsyncResult(app=app)
        child._state = _pending_until(time.monotonic() + 10)
        parent = _task_leaving_a_child_pending(app, child)

        # the run is over, so revoking what is left of it needs a broker that a
        # unit test has no business standing up.
        with mock.patch.object(FireXTask, "revoke_nonready_children"):
            parent()

        assert "Giving up waiting for this task's children" in caplog.text

    def test_a_child_is_waited_for_while_the_run_still_has_time(self, caplog):
        app = _app_with_run_started(hours_ago=0)
        state_reads = itertools.count(1)
        child = MockFxAsyncResult(app=app)
        child._state = lambda: PENDING if next(state_reads) < 3 else SUCCESS
        parent = _task_leaving_a_child_pending(app, child)

        parent()

        assert next(state_reads) > 3, "completed without waiting for its child"
        assert "Giving up waiting" not in caplog.text


@pytest.mark.parametrize("hours_ago", [0, 5])
def test_a_task_without_children_waits_for_nothing(hours_ago):
    app = _app_with_run_started(hours_ago=hours_ago)

    @app.task(base=FireXTask)
    def childless():
        pass

    # notably including a run that is out of time: there is nothing to give up on.
    assert childless() == {}
