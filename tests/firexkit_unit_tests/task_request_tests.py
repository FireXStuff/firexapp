"""
A task whose process dies never reaches its own postrun, so the worker's controlling
process records that it has stopped running, and tells event listeners, instead.
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

from firexapp.events.model import TASK_REVOKE_REASON_KEY, RevokeDetails, RunStates
from firexkit.firex_celery import FireXCelery, _update_task_name
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


class _Eventer:
    """The worker's event dispatcher, minus the broker: it keeps what it's sent."""

    enabled = True

    def __init__(self):
        self.sent = []

    def send(self, event_type, uuid, **fields):
        self.sent.append({"type": event_type, "uuid": uuid, **fields})

    def completion_events(self) -> list[dict]:
        return [
            event
            for event in self.sent
            if event["type"]
            in ("task-completed", RunStates.REVOKE_COMPLETED.to_celery_event_type())
        ]


@pytest.fixture
def pool() -> _Pool:
    return _Pool()


@pytest.fixture
def eventer() -> _Eventer:
    return _Eventer()


@pytest.fixture
def task(ut_app: FireXCelery) -> FireXTask:
    @ut_app.task(base=FireXTask)
    def a_task():
        pass

    return a_task


def _start(
    app: FireXCelery, task: FireXTask, pool: _Pool, eventer: _Eventer | None = None
):
    """Hand task to the pool the way the worker does, and have the pool start it."""
    request = symbol_by_name(task.Request)(
        TaskMessage(task.name), app=app, task=task, eventer=eventer
    )
    job = request.execute_using_pool(pool)
    job._ack(None, monotonic(), _PID, None)  # its process accepts it,
    _update_task_name(task, request.id)  # and starts it.
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

    request.terminate(pool, signal="SIGKILL")
    assert pool.terminated_pids == [_PID]
    assert _stored_state(ut_app, request) == REVOKED
    # until the signal lands, the task is still going.
    assert _is_running(ut_app, request)

    job._set_terminated(-signal.SIGKILL)  # as the pool does once it reaps the process.
    assert not _is_running(ut_app, request)


def test_a_task_whose_process_dies_stops_running(ut_app, task, pool):
    request, job = _start(ut_app, task, pool)

    pool.mark_as_worker_lost(job, -signal.SIGKILL)

    assert not _is_running(ut_app, request)
    assert _stored_state(ut_app, request) == FAILURE


def test_listeners_hear_a_task_whose_process_dies_is_complete(
    ut_app, task, pool, eventer
):
    request, job = _start(ut_app, task, pool, eventer)

    pool.mark_as_worker_lost(job, -signal.SIGKILL)

    [completed] = eventer.completion_events()
    assert completed["type"] == "task-completed"
    assert completed["uuid"] == request.id
    assert completed["actual_runtime"] >= 0


def test_listeners_hear_a_task_killed_by_revoke_is_revoke_complete(
    ut_app, tmp_path, task, pool, eventer
):
    ut_app.conf.logs_dir = str(tmp_path)
    request, job = _start(ut_app, task, pool, eventer)
    revoke_request = RevokeDetails(
        str(tmp_path), reason="not needed", task_uuid=request.id, root_revoke=False
    )
    revoke_request.write()

    request.terminate(pool, signal="SIGKILL")
    # until the signal lands, the task is still going.
    assert eventer.completion_events() == []

    job._set_terminated(-signal.SIGKILL)

    revoke_completed, completed = eventer.completion_events()
    assert revoke_completed == {
        "type": RunStates.REVOKE_COMPLETED.to_celery_event_type(),
        "uuid": request.id,
        TASK_REVOKE_REASON_KEY: "not needed",
    }
    assert completed["type"] == "task-completed"
    assert ut_app.get_task_revoke_details(request.id).is_revoke_completed()


def test_a_task_whose_process_reached_its_postrun_is_left_to_it(
    ut_app, task, pool, eventer
):
    request, job = _start(ut_app, task, pool, eventer)

    # A revoke's first SIGTERM lets its process run its postrun before it exits.
    request.terminate(pool)
    ut_app.mark_task_postrun(request.id)
    job._set_terminated(-signal.SIGTERM)

    assert not _is_running(ut_app, request)
    assert eventer.completion_events() == []


def test_a_task_requeued_when_its_process_dies_is_left_to_run_again(
    ut_app, pool, eventer
):
    @ut_app.task(base=FireXTask, acks_late=True, reject_on_worker_lost=True)
    def requeued():
        pass

    request, job = _start(ut_app, requeued, pool, eventer)

    pool.mark_as_worker_lost(job, -signal.SIGKILL)

    # it runs again under the same id, so it's that run's postrun to record.
    assert not ut_app.task_id_has_postrun(request.id)
    assert eventer.completion_events() == []


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

    request.terminate(pool, signal="SIGKILL")
    job._set_terminated(-signal.SIGKILL)

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
