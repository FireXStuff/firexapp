"""
Revoking a task that has started waits until it has both stopped running and stored
how it ended, which a revoke's SIGTERM can make visible in either order.
"""

import threading
import time
from collections.abc import Callable
from unittest import TestCase, mock

from celery.states import PENDING, REVOKED, STARTED

from firexkit.firex_celery import _TASK_PRE_RUN_KEY, FireXCelery
from firexkit.result import ChainRevokedException, FxAsyncResult, ManyFxAsyncResults
from firexkit.testing import MockFxAsyncResult, ut_backed_celery_app

# Long enough that the revoke has to be waited out, short enough not to slow the suite.
_WORKER_DELAY = 0.2
_MAX_WAIT = 30


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


class RevokeNonReadyTests(TestCase):
    def setUp(self):
        self.task = _Task(ut_backed_celery_app())
        self.addCleanup(self.task.join)

    def test_waits_for_revoked_to_be_stored_after_the_task_is_postrun(self):
        self.task.start()

        def worker_on_revoke():
            # The task finishes cleanup before its worker stores REVOKED.
            self.task.postrun()
            self.task.later(self.task.store_revoked)

        revoked, waited = _revoke_non_ready(self.task, worker_on_revoke)

        self.assertEqual(revoked, [self.task.result])
        self.assertTrue(revoked[0].fx_is_revoked())
        self.assertGreaterEqual(waited, _WORKER_DELAY)

    def test_waits_for_the_task_to_be_postrun_after_revoked_is_stored(self):
        self.task.start()

        def worker_on_revoke():
            # Sending SIGTERM stores REVOKED before the process finishes cleanup.
            self.task.store_revoked()
            self.task.later(self.task.postrun)

        revoked, waited = _revoke_non_ready(self.task, worker_on_revoke)

        self.assertEqual(revoked, [self.task.result])
        self.assertFalse(revoked[0].fx_is_running())
        self.assertTrue(revoked[0].fx_is_revoked())
        self.assertGreaterEqual(waited, _WORKER_DELAY)

    def test_does_not_wait_on_a_task_that_never_started(self):
        # No process needs to stop; publishing a revoke need not wait for its reply.
        revoked, waited = _revoke_non_ready(self.task, lambda: None)

        self.assertEqual(revoked, [self.task.result])
        self.assertLess(waited, _WORKER_DELAY)
        self.assertFalse(self.task.result.fx_is_ready())

    def test_gives_up_on_a_task_that_never_stores_how_it_ended(self):
        self.task.start()

        revoked, waited = _revoke_non_ready(self.task, self.task.postrun, max_wait=0.1)

        self.assertEqual(revoked, [self.task.result])
        self.assertGreaterEqual(waited, 0.1)
        self.assertFalse(revoked[0].fx_is_ready())

    def test_waiting_on_a_revoked_result_still_waits_for_running_cleanup(self):
        self.task.start()
        self.task.store_revoked()
        self.task.later(self.task.postrun)
        start = time.monotonic()

        with self.assertRaises(ChainRevokedException) as raised:
            self.task.result.fx_wait_no_state_update(max_wait=1)

        self.assertIs(type(raised.exception), ChainRevokedException)
        self.assertGreaterEqual(time.monotonic() - start, _WORKER_DELAY)
        self.assertFalse(self.task.result.fx_is_running())
