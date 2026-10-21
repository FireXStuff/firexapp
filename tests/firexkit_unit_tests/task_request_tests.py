"""
A task whose process dies never reaches its own postrun, so the worker's controlling
process records that it has stopped running instead.
"""

import signal
from time import monotonic
from unittest import mock

import pytest
from billiard.einfo import ExceptionInfo
from billiard.pool import ApplyResult, TimeoutHandler
from billiard.pool import Pool as BilliardPool
from celery.contrib.testing.mocks import TaskMessage
from celery.exceptions import WorkerLostError
from celery.states import FAILURE, REVOKED
from kombu.utils.imports import symbol_by_name

from firexkit.firex_celery import _TASK_PRE_RUN_KEY, FireXCelery
from firexkit.task import FireXTask, FireXTaskRequest
from firexkit.testing import UtClient

# The pool process the task is handed to.
_PID = 4242


class _Pool:
    """The worker's process pool, minus its processes: the tests play their part."""

    def __init__(self):
        self.terminated_pids = []

    def apply_async(
        self,
        _trace,
        *,
        accept_callback,
        timeout_callback,
        callback,
        error_callback,
        timeout,
        correlation_id,
        **_kwargs,
    ):
        return ApplyResult(
            {},
            callback,
            accept_callback=accept_callback,
            timeout_callback=timeout_callback,
            error_callback=error_callback,
            timeout=timeout,
            correlation_id=correlation_id,
        )

    def terminate_job(self, pid, _signal=None):
        self.terminated_pids.append(pid)

    # billiard's own, which only needs the job.
    mark_as_worker_lost = BilliardPool.mark_as_worker_lost


@pytest.fixture
def pool() -> _Pool:
    return _Pool()


@pytest.fixture
def task(ut_app: FireXCelery) -> FireXTask:
    @ut_app.task(base=FireXTask)
    def a_task():
        pass

    return a_task


def _start(app: FireXCelery, task: FireXTask, pool: _Pool):
    """Hand task to the pool the way the worker does, and have the pool start it."""
    request = symbol_by_name(task.Request)(TaskMessage(task.name), app=app, task=task)
    job = request.execute_using_pool(pool)
    job._ack(None, monotonic(), _PID, None)  # its process accepts it,
    app.backend_hset_task_attr(request.id, _TASK_PRE_RUN_KEY, "True")  # and starts it.
    return request, job


def _is_running(app: FireXCelery, request: FireXTaskRequest) -> bool:
    return app.AsyncResult(request.id).fx_is_running()


def _stored_state(app: FireXCelery, request: FireXTaskRequest) -> str | None:
    state = app.backend.client.get(app.backend.get_key_for_task(request.id))
    return state.decode() if state is not None else None


def _raised(exc: BaseException) -> ExceptionInfo:
    try:
        raise exc
    except type(exc):
        return ExceptionInfo()


def test_a_firex_task_has_its_process_death_seen_to(task):
    assert issubclass(symbol_by_name(task.Request), FireXTaskRequest)


def test_a_task_killed_by_revoke_stops_running_once_its_process_exits(
    ut_app, task, pool
):
    request, job = _start(ut_app, task, pool)

    request.terminate(pool)
    assert pool.terminated_pids == [_PID]
    assert _stored_state(ut_app, request) == REVOKED
    # until the signal lands, the task is still going.
    assert _is_running(ut_app, request)

    job._set_terminated(-signal.SIGTERM)  # as the pool does once it reaps the process.
    assert not _is_running(ut_app, request)


def test_a_task_whose_process_dies_stops_running(ut_app, task, pool):
    request, job = _start(ut_app, task, pool)

    pool.mark_as_worker_lost(job, -signal.SIGKILL)

    assert not _is_running(ut_app, request)
    assert _stored_state(ut_app, request) == FAILURE


def test_a_task_requeued_when_its_process_dies_is_left_to_run_again(ut_app, pool):
    @ut_app.task(base=FireXTask, acks_late=True, reject_on_worker_lost=True)
    def requeued():
        pass

    request, job = _start(ut_app, requeued, pool)

    pool.mark_as_worker_lost(job, -signal.SIGKILL)

    # it runs again under the same id, so it's that run's postrun to record.
    assert not ut_app.task_id_has_postrun(request.id)


def test_a_task_past_its_hard_time_limit_runs_until_its_process_is_killed(
    ut_app, task, pool
):
    request, job = _start(ut_app, task, pool)

    # The pool fails the task before it kills its process, which there is none of here.
    TimeoutHandler([], {}, None, None).on_hard_timeout(job)

    assert _stored_state(ut_app, request) == FAILURE
    assert _is_running(ut_app, request)


def test_a_killed_task_that_asked_to_be_forgotten_is_forgotten(ut_app, task, pool):
    request, job = _start(ut_app, task, pool)
    ut_app.AsyncResult(request.id).set_fx_forget()

    request.terminate(pool)
    job._set_terminated(-signal.SIGTERM)

    assert ut_app.AsyncResult(request.id).fx_is_forgotten()
    assert _stored_state(ut_app, request) is None


def test_the_worker_carries_on_when_it_cant_record_it(
    ut_app, ut_client: UtClient, task, pool
):
    request, _job = _start(ut_app, task, pool)

    with mock.patch.object(
        ut_client, "hset", side_effect=RuntimeError("redis is gone")
    ):
        request.on_failure(_raised(WorkerLostError("Worker exited prematurely")))

    assert _stored_state(ut_app, request) == FAILURE
