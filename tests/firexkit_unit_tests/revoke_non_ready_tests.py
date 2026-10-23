"""
Revoking a task that has started waits until it has both stopped running and stored
how it ended, which a revoke's SIGTERM can make visible in either order.
"""

import threading
import time
from collections.abc import Callable, Generator
from unittest import mock

import pytest
from celery.states import PENDING, REVOKED, STARTED

from firexkit.firex_celery import _TASK_PRE_RUN_KEY, FireXCelery
from firexkit.result import FxAsyncResult, ManyFxAsyncResults
from firexkit.revoke import RevokedRequests
from firexkit.testing import MockFxAsyncResult

# Long enough that the revoke has to be waited out, short enough not to slow the suite.
_WORKER_DELAY = 0.2
_MAX_WAIT = 30


@pytest.fixture(autouse=True)
def _revokes_known_only_from_the_backend() -> Generator[None]:
    # A stale RevokedRequests would ask the workers, of which there are none.
    prev_instance = RevokedRequests._instance
    RevokedRequests._instance = RevokedRequests(timer_expiry_secs=_MAX_WAIT * 100)
    try:
        yield
    finally:
        RevokedRequests._instance = prev_instance


class _Task:
    """A task as its worker records it in the backend."""

    def __init__(self, app: FireXCelery):
        self.app = app
        self.state = PENDING
        self.result = MockFxAsyncResult(state=lambda: self.state, app=app)
        self._timers: list[threading.Timer] = []

    def start(self) -> None:
        self.app.backend_hset_task_attr(self.result.id, _TASK_PRE_RUN_KEY, "True")
        self.state = STARTED

    def store_revoked(self) -> None:
        self.state = REVOKED

    def postrun(self) -> None:
        self.app.mark_task_postrun(self.result.id)

    def later(self, step: Callable[[], None]) -> None:
        timer = threading.Timer(_WORKER_DELAY, step)
        self._timers.append(timer)
        timer.start()

    def join(self) -> None:
        for timer in self._timers:
            timer.join()


@pytest.fixture
def task(ut_app: FireXCelery) -> Generator[_Task]:
    task = _Task(ut_app)
    yield task
    task.join()


@pytest.fixture
def started(task: _Task) -> _Task:
    task.start()
    return task


def _revoke_non_ready(
    task: _Task,
    worker_on_revoke: Callable[[], None],
    max_wait: float = _MAX_WAIT,
) -> tuple[list[FxAsyncResult], float]:
    with mock.patch.object(
        task.result, "revoke", side_effect=lambda **_: worker_on_revoke()
    ):
        start = time.monotonic()
        revoked = ManyFxAsyncResults.create_fx_ars(task.result).revoke_non_ready(
            max_wait=max_wait
        )
    return list(revoked), time.monotonic() - start


def test_waits_for_revoked_to_be_stored_after_the_task_is_postrun(started: _Task):
    def worker_on_revoke():
        # The SIGTERM lands first: the task's process gets through its postrun
        # before its worker gets as far as storing REVOKED.
        started.postrun()
        started.later(started.store_revoked)

    revoked, _ = _revoke_non_ready(started, worker_on_revoke)

    assert revoked == [started.result]
    assert revoked[0].fx_is_revoked()


def test_waits_for_the_task_to_be_postrun_after_revoked_is_stored(started: _Task):
    def worker_on_revoke():
        # The usual order: the worker stores REVOKED as soon as it has sent the
        # SIGTERM, while the task's process is still on its way out.
        started.store_revoked()
        started.later(started.postrun)

    revoked, _ = _revoke_non_ready(started, worker_on_revoke)

    assert revoked == [started.result]
    assert not revoked[0].fx_is_running()
    assert revoked[0].fx_is_revoked()


def test_does_not_wait_on_a_task_that_never_started(task: _Task):
    # Its worker only stores REVOKED once it dequeues the task, if it ever does.
    revoked, waited = _revoke_non_ready(task, lambda: None)

    assert revoked == [task.result]
    assert waited < _WORKER_DELAY


def test_gives_up_on_a_task_that_never_stores_how_it_ended(started: _Task):
    revoked, waited = _revoke_non_ready(started, started.postrun, max_wait=0.1)

    assert revoked == [started.result]
    assert waited >= 0.1
    assert not revoked[0].fx_is_ready()
