import datetime
import time
import unittest
from collections.abc import Callable, Generator
from contextlib import contextmanager
from unittest import mock

import pytest
from celery.states import FAILURE, PENDING, REVOKED, STARTED, SUCCESS

from firexkit.firex_celery import FireXCelery
from firexkit.result import (
    ChainInterruptedByZombieTaskException,
    ChainInterruptedException,
    ChainRevokedException,
    ManyFxAsyncResults,
    MultipleFailuresException,
    WaitLoopCallBack,
    WaitOnChainTimeoutError,
    find_unsuccessful_in_chain,
    first_non_chain_interrupted_exception,
    last_causing_chain_interrupted_exception,
    wait_on_async_results,
)
from firexkit.revoke import RevokedRequests, _now_utc
from firexkit.run_time import RunTimeReserve
from firexkit.testing import MockFxAsyncResult, ut_backed_celery_app


def get_mocks(
    result_ids=None,
    _state=None,
) -> tuple[FireXCelery, list[MockFxAsyncResult]]:
    if result_ids is None:
        result_ids = ["anything"]

    test_app = ut_backed_celery_app()
    mock_results = [
        MockFxAsyncResult(state=_state, id=r, app=test_app) for r in result_ids
    ]
    return test_app, mock_results


def setup_revoke(revoked=()):
    RevokedRequests._instance = RevokedRequests(revoked)
    # disable update
    RevokedRequests._instance.last_updated = _now_utc() + datetime.timedelta(days=1)


class ResultsLoggingNamesTests(unittest.TestCase):
    def test_get_task_name(self):
        result_id = "anything"
        test_app, mock_result = get_mocks([result_id])
        mock_result = mock_result[0]
        mock_result._name = None

        with mock.patch.object(
            test_app.backend.client, "hget", create=True, return_value=b"yes"
        ):
            self.assertEqual(mock_result.fx_get_name(), "yes")

    def test_get_logging_name(self):
        _test_app, mock_result = get_mocks()
        self.assertEqual(mock_result[0].fx_logging_name(), "[anything]")


class ResultsReadyTests(unittest.TestCase):
    def test_is_ready(self):
        _test_app, mock_result = get_mocks()
        mock_result = mock_result[0]
        mock_result._state = SUCCESS
        self.assertTrue(mock_result.fx_is_ready())

        mock_result = get_mocks()[1][0]
        mock_result._state = FAILURE
        self.assertTrue(mock_result.fx_is_ready())

        mock_result = get_mocks()[1][0]
        mock_result._state = REVOKED
        self.assertTrue(mock_result.fx_is_ready())

        mock_result = get_mocks()[1][0]
        mock_result._state = STARTED
        self.assertFalse(mock_result.fx_is_ready())

    def test_backend_exception(self):
        _test_app, mock_result = get_mocks()
        mock_result = mock_result[0]

        # exceptions go up the stack
        def bad_backend():
            raise AssertionError()

        mock_result._state = bad_backend
        with self.assertRaises(AssertionError):
            mock_result.fx_is_ready()

        # exceptions go up the stack even if trials are enabled
        def bad_backend():
            mock_result._state = SUCCESS
            raise AssertionError()

        mock_result._state = bad_backend
        with self.assertRaises(AssertionError):
            mock_result.fx_is_ready(timeout=5)

        # Timeouts try again
        def bad_backend():
            mock_result._state = SUCCESS
            raise TimeoutError()

        mock_result._state = bad_backend
        self.assertTrue(mock_result.fx_is_ready())

        # Timeouts try again only a few times
        mock_result = get_mocks()[1][0]

        def bad_backend():
            raise TimeoutError()

        mock_result._state = bad_backend
        with self.assertRaises(TimeoutError):
            mock_result.fx_is_ready(timeout=3)


class WaitOnResultsTests(unittest.TestCase):
    def test_wait_on_nothing(self):
        # make sure the function returns, although in doesn't return anything
        self.assertIsNone(wait_on_async_results(None))

    def test_wait_on_single_result(self):
        setup_revoke()
        _test_app, mock_result = get_mocks()
        mock_result = mock_result[0]

        mock_result._state = SUCCESS
        self.assertIsNone(wait_on_async_results(mock_result))

        # wait then go
        def wait_and_go():
            mock_result._state = SUCCESS
            return STARTED

        mock_result._state = wait_and_go
        try:
            self.assertIsNone(wait_on_async_results(mock_result))
        finally:
            mock_result.backend = None

    @contextmanager
    def prime_mocks(self, mock_results, expected_hits: int):
        hits = []
        for r in mock_results:

            def wait_and_go(r1=r):
                def started_and_go(r2=r1):
                    r2._state = SUCCESS
                    hits.append(r2)
                    return STARTED

                r1._state = started_and_go
                return PENDING

            r._state = wait_and_go
        yield

        self.assertEqual(len(hits), expected_hits)

    def test_wait_on_many_results(self):
        setup_revoke()
        _test_app, mock_results = get_mocks(["a1", "a2", "a3"])

        with self.prime_mocks(mock_results, 3):
            self.assertIsNone(wait_on_async_results(mock_results))

    def test_wait_on_chain(self):
        setup_revoke()
        _test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockFxAsyncResult.set_heritage(mock_results[1], mock_results[2])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])

        with self.prime_mocks(mock_results, 3):
            self.assertIsNone(wait_on_async_results(mock_results[2]))

        unsuccessful = find_unsuccessful_in_chain(mock_results[-1])
        self.assertDictEqual(unsuccessful, {})

    def test_self_parent_recursion(self):
        setup_revoke()
        _test_app, mock_result = get_mocks(_state=SUCCESS)
        mock_result = mock_result[0]
        MockFxAsyncResult.set_heritage(mock_result, mock_result)
        self.assertIsNone(wait_on_async_results(mock_result))

    def test_callbacks(self):
        setup_revoke()
        _test_app, mock_result = get_mocks()
        mock_result = mock_result[0]
        mock_result._state = STARTED

        def call_this():
            call_this.was_called += 1
            if call_this.was_called == 5:
                mock_result._state = SUCCESS

        call_this.was_called = 0

        callbacks = [WaitLoopCallBack(func=call_this, frequency=0.2, kwargs={})]
        self.assertIsNone(wait_on_async_results(mock_result, callbacks=callbacks))
        self.assertEqual(call_this.was_called, 5)

    def test_Chain_interrupted(self):
        setup_revoke()
        _test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockFxAsyncResult.set_heritage(mock_results[1], mock_results[2])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])
        mock_results[0]._state = SUCCESS
        mock_results[1]._state = FAILURE
        mock_results[2]._state = PENDING
        with self.assertRaises(ChainInterruptedException) as context:
            print("will wait on ar")
            wait_on_async_results(mock_results[2], max_wait=1)
            print("done wait on ar")
        self.assertIsNone(context.exception.__cause__)

        unsuccessful = find_unsuccessful_in_chain(mock_results[-1])
        self.assertDictEqual(
            unsuccessful, {"not_run": [mock_results[2]], "failed": [mock_results[1]]}
        )

    def test_Chain_interrupted_from_exc(self):
        setup_revoke()
        _test_app, mock_results = get_mocks(["a0", "a1"])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])
        mock_results[0]._state = SUCCESS
        mock_results[1]._state = FAILURE
        mock_results[1]._result = OSError()
        with self.assertRaises(ChainInterruptedException) as context:
            wait_on_async_results(mock_results[1])
        self.assertTrue(isinstance(context.exception.__cause__, OSError))

    def test_chain_interrupted_task_name_is_celery_name_not_logging_name(self):
        setup_revoke()
        fail_name = "microservices.lumens_tasks.LumensQuery"
        fail_uuid = "d4f7b612-f497-4f2c-9b21-9fa8075d12b4"
        cause = Exception("Lumens returned status code: <Response [400]>")
        _test_app, mock_results = get_mocks([fail_uuid])
        ar = mock_results[0]
        ar._state = FAILURE
        ar._result = cause
        ar._fx_name = fail_name
        with (
            mock.patch.object(ar, "fx_backend_get_name", return_value=fail_name),
            self.assertRaises(ChainInterruptedException) as context,
        ):
            wait_on_async_results(ar)
        e = context.exception
        self.assertEqual(e.task_id, fail_uuid)
        self.assertEqual(
            e.task_name,
            fail_name,
            f"task_name should be the celery name, not the logging name; got {e.task_name!r}",
        )
        self.assertEqual(
            repr(e),
            f"ChainInterruptedException('{fail_uuid}', '{fail_name}', {cause!r})",
        )
        self.assertEqual(
            str(e),
            "The chain has been interrupted by a failure in microservice "
            f"{fail_name}[{fail_uuid}]",
        )

    def test_timeout(self):
        setup_revoke()
        _test_app, mock_result = get_mocks()
        mock_result = mock_result[0]
        mock_result._state = STARTED
        with self.assertRaises(WaitOnChainTimeoutError):
            wait_on_async_results(mock_result, max_wait=0.2)

    def test_wait_on_revoked_chain(self):
        setup_revoke()
        _test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockFxAsyncResult.set_heritage(mock_results[1], mock_results[2])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])

        for i in range(3):
            # middle of the chain is revoked
            mock_results[0]._state = SUCCESS
            mock_results[1]._state = SUCCESS
            mock_results[2]._state = STARTED

            mock_results[i]._state = REVOKED
            with self.assertRaises(ChainRevokedException):
                wait_on_async_results(mock_results[2])

    def test_wait_on_revoked_result(self):

        _test_app, mock_result = get_mocks(["rev"])
        mock_result = mock_result[0]
        setup_revoke(["rev"])
        mock_result._state = PENDING
        with self.assertRaises(ChainRevokedException):
            wait_on_async_results(mock_result)

        unsuccessful = find_unsuccessful_in_chain(mock_result)
        self.assertDictEqual(unsuccessful, {"not_run": [mock_result]})

    def test_wait_for_all_even_on_failure(self):
        setup_revoke()
        _test_app, mock_results = get_mocks(["a0", "a1", "a2"])

        with self.prime_mocks(mock_results, 2):
            # a0 and a2 should both be hit, but not a1
            mock_results[1]._state = FAILURE
            mock_results[1]._result = OSError()
            with self.assertRaises(ChainInterruptedException):
                wait_on_async_results(mock_results)

    def test_multiple_failures(self):
        setup_revoke()
        mock_results = get_mocks(["a0", "a1", "a2", "a3"])[1]

        print(f"Initial state of first: {mock_results[0].state}")
        mock_results[0]._state = SUCCESS

        # with self.prime_mocks(mock_results, 1):
        # a0 and a1 should both have failure
        mock_results[1]._state = FAILURE
        mock_results[2]._state = PENDING
        mock_results[3]._state = FAILURE

        # make a1-a2 a chain, so a2 will not be hit
        MockFxAsyncResult.set_heritage(
            parent=mock_results[1],
            child=mock_results[2],
        )
        mock_results.remove(mock_results[1])

        with self.assertRaises(MultipleFailuresException) as multi_failure:
            wait_on_async_results(mock_results, max_wait=3)

        multi_failure_exception = multi_failure.exception
        self.assertEqual(len(multi_failure_exception.failures), 2)
        self.assertTrue(
            isinstance(multi_failure_exception.failures[0], ChainInterruptedException)
        )
        self.assertTrue(
            isinstance(multi_failure_exception.failures[1], ChainInterruptedException)
        )


class WalkExceptionTests(unittest.TestCase):
    def test_last_chain_interrupted(self):

        e1 = Exception("exception1")
        try:
            raise ChainInterruptedException("exception2") from e1
        except ChainInterruptedException as e:
            e2 = e

        try:
            raise ChainInterruptedException("exception3") from e2
        except ChainInterruptedException as e:
            e3 = e

        try:
            raise ChainInterruptedException("exception3") from e3
        except ChainInterruptedException as e:
            e4 = e

        last_cause = last_causing_chain_interrupted_exception(e4)
        self.assertIs(e2, last_cause)

    def test_fail_double_monkey_patch_track(self):
        e1 = Exception("exception1")
        try:
            raise ChainInterruptedException("exception2") from e1
        except ChainInterruptedException as e:
            e2 = e

        try:
            raise ChainInterruptedException("exception3") from e2
        except ChainInterruptedException as e:
            e3 = e

        non_chain_interrupted = first_non_chain_interrupted_exception(e3)
        self.assertIs(e1, non_chain_interrupted)


class RunRelativeWaitTests(unittest.TestCase):
    """
    The case the run time budget exists for: a task blocked on children allowed to
    run for nearly the whole run, whose wait must move when the budget does.
    """

    def setUp(self):
        setup_revoke()
        self.test_app, mock_results = get_mocks(["long-running"])
        self.result = mock_results[0]
        self.result._state = STARTED
        # Stands in for the deadline derived from the budget held in the result backend.
        self.run_deadline = time.time() + 0.2

    @contextmanager
    def patched_run_deadline(self):
        with mock.patch(
            "firexkit.run_time.get_run_deadline",
            side_effect=lambda app=None: self.run_deadline,
        ):
            yield

    def wait(self, max_wait, **kwargs):
        with self.patched_run_deadline():
            wait_on_async_results(self.result, max_wait=max_wait, **kwargs)

    def test_gives_up_when_the_run_is_out_of_time(self):
        with self.assertRaises(WaitOnChainTimeoutError):
            self.wait(RunTimeReserve(0))

    def test_the_reserve_shortens_the_wait(self):
        # Wants 10s left at the end of a run with 0.2s to go: already out of time.
        started = time.time()
        with self.assertRaises(WaitOnChainTimeoutError):
            self.wait(RunTimeReserve(10))
        self.assertLess(time.time() - started, 0.2)

    def test_a_budget_raised_mid_wait_extends_an_already_blocked_waiter(self):
        # The heart of the feature: nothing signals the blocked wait, it just
        # re-resolves the run deadline on every poll.
        original_deadline = self.run_deadline

        def raise_the_budget_then_complete():
            now = time.time()
            if now > original_deadline - 0.1 and self.run_deadline == original_deadline:
                # Some task discovered the run needs longer and raised the budget.
                self.run_deadline = now + 5
            elif now > original_deadline + 0.3:
                # Well past the point this wait would originally have given up.
                self.result._state = SUCCESS
            return self.result._state

        self.result._state = raise_the_budget_then_complete

        self.wait(RunTimeReserve(0))

        self.assertEqual(SUCCESS, self.result.state)

    def test_a_plain_max_wait_ignores_the_budget(self):
        self.run_deadline = time.time() + 3600
        with self.assertRaises(WaitOnChainTimeoutError):
            self.wait(0.2)


@pytest.fixture
def no_revoke_updates() -> Generator[None]:
    # A stale RevokedRequests would ask the workers, of which there are none.
    prev_instance = RevokedRequests._instance
    setup_revoke()
    try:
        yield
    finally:
        RevokedRequests._instance = prev_instance


@pytest.fixture
def always_dead_workers() -> Generator[list[MockFxAsyncResult]]:
    """Every worker check finds the worker dead, recording the result checked."""
    checked = []

    def is_worker_alive(result):
        checked.append(result)
        return False

    with (
        mock.patch("firexkit.result._CHECK_TASK_WORKER_FREQ", 0),
        mock.patch("firexkit.result._is_worker_alive", side_effect=is_worker_alive),
    ):
        yield checked


def _completes_in(seconds: float, state: str = SUCCESS) -> Callable[[], str]:
    completes_at = time.monotonic() + seconds
    return lambda: state if time.monotonic() >= completes_at else STARTED


@pytest.mark.usefixtures("no_revoke_updates")
class TestGetAsCompleted:
    def test_yields_results_in_the_order_they_complete(self, ut_app):
        last, first, second = (
            MockFxAsyncResult(state=_completes_in(s), app=ut_app)
            for s in (0.9, 0.1, 0.5)
        )

        completed = ManyFxAsyncResults.fx_ars_from_list([last, first, second])

        assert list(completed.get_as_completed(max_wait=30)) == [first, second, last]

    def test_leaves_the_results_it_was_given_intact(self, ut_app):
        given = [MockFxAsyncResult(state=SUCCESS, app=ut_app) for _ in range(2)]
        ars = ManyFxAsyncResults.fx_ars_from_list(given)

        assert list(ars.get_as_completed(max_wait=30)) == given
        assert list(ars) == given

    def test_a_completion_is_noticed_however_many_others_are_running(self, ut_app):
        running = [MockFxAsyncResult(state=STARTED, app=ut_app) for _ in range(50)]
        completes = MockFxAsyncResult(state=_completes_in(0.5), app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_list(running + [completes])

        start = time.monotonic()
        assert next(ars.get_as_completed(max_wait=60)) == completes
        # Waiting on each running result in turn had taken over 10s to get to it.
        assert time.monotonic() - start < 3

    def test_gives_up_after_max_wait_however_many_are_running(self, ut_app):
        running = [MockFxAsyncResult(state=STARTED, app=ut_app) for _ in range(50)]
        ars = ManyFxAsyncResults.fx_ars_from_list(running)

        start = time.monotonic()
        with pytest.raises(WaitOnChainTimeoutError) as exc_info:
            next(ars.get_as_completed(max_wait=0.5))
        assert time.monotonic() - start < 3
        # Before the results it names, which are too many for a truncated message.
        assert str(exc_info.value).startswith("Still not ready after 0.5 seconds: ")

    def test_callbacks_run_on_their_own_clock(self, ut_app):
        calls = []
        ar = MockFxAsyncResult(
            state=lambda: SUCCESS if len(calls) >= 3 else STARTED, app=ut_app
        )
        callbacks = [
            WaitLoopCallBack(func=lambda: calls.append(1), frequency=0.2, kwargs={})
        ]
        ars = ManyFxAsyncResults.fx_ars_from_single(ar)

        start = time.monotonic()
        assert list(ars.get_as_completed(max_wait=30, callbacks=callbacks)) == [ar]
        # Restarting the callback clock on every check of a result had kept a
        # callback this frequent from running for over 10s.
        assert time.monotonic() - start < 3

    def test_without_raise_on_failure_revoked_and_failed_are_yielded(self, ut_app):
        revoked = MockFxAsyncResult(state=REVOKED, app=ut_app)
        failed = MockFxAsyncResult(state=FAILURE, result=OSError(), app=ut_app)
        succeeds = MockFxAsyncResult(state=_completes_in(0.3), app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_list([revoked, failed, succeeds])

        completed = ars.get_keyed_as_completed(max_wait=30, raise_on_failure=False)

        assert list(completed) == [(0, revoked), (1, failed), (2, succeeds)]

    def test_with_raise_on_failure_a_revoke_raises(self, ut_app):
        revoked = MockFxAsyncResult(state=REVOKED, app=ut_app)
        running = MockFxAsyncResult(state=STARTED, app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_list([running, revoked])

        with pytest.raises(ChainRevokedException):
            next(ars.get_as_completed(max_wait=30))

    def test_results_completed_ahead_of_a_failure_are_yielded_first(self, ut_app):
        succeeded = MockFxAsyncResult(state=SUCCESS, app=ut_app)
        failed = MockFxAsyncResult(state=FAILURE, result=OSError(), app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_list([succeeded, failed])

        completed = ars.get_as_completed(max_wait=30)

        assert next(completed) == succeeded
        with pytest.raises(ChainInterruptedException):
            next(completed)

    def test_a_failed_chain_parent_completes_its_child(self, ut_app):
        parent = MockFxAsyncResult(state=FAILURE, result=OSError(), app=ut_app)
        child = MockFxAsyncResult(state=PENDING, app=ut_app)
        MockFxAsyncResult.set_heritage(parent, child)
        ars = ManyFxAsyncResults.fx_ars_from_single(child)

        assert list(ars.get_as_completed(max_wait=30, raise_on_failure=False)) == [
            child
        ]
        with pytest.raises(ChainInterruptedException) as raised:
            next(ars.get_as_completed(max_wait=30))
        assert raised.value.task_id == parent.id

    def test_only_the_first_result_waited_on_is_checked_for_being_a_zombie(
        self, ut_app, always_dead_workers
    ):
        first, second = (MockFxAsyncResult(state=STARTED, app=ut_app) for _ in "12")
        ars = ManyFxAsyncResults.fx_ars_from_list([first, second])

        completed = ars.get_as_completed(max_wait=30, raise_on_failure=False)

        assert list(completed) == [first, second]
        # As wait_for_all would, second is only checked once first is done with.
        assert always_dead_workers == [first] * 3 + [second] * 3

    def test_with_raise_on_failure_a_zombie_raises(self, ut_app, always_dead_workers):
        zombie = MockFxAsyncResult(state=STARTED, app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_single(zombie)

        with pytest.raises(ChainInterruptedByZombieTaskException):
            next(ars.get_as_completed(max_wait=30))

    def test_wait_for_any_raises_a_revoke_without_raise_on_failure(self, ut_app):
        revoked = MockFxAsyncResult(state=REVOKED, app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_single(revoked)

        with pytest.raises(ChainRevokedException):
            ars.wait_for_any(max_wait=30, raise_on_failure=False)

    def test_wait_for_any_returns_a_failure_without_raise_on_failure(self, ut_app):
        failed = MockFxAsyncResult(state=FAILURE, result=OSError(), app=ut_app)
        ars = ManyFxAsyncResults.fx_ars_from_single(failed)

        assert ars.wait_for_any(max_wait=30, raise_on_failure=False) == failed


@pytest.mark.usefixtures("no_revoke_updates")
def test_wait_for_all_gives_up_on_a_zombie(ut_app, always_dead_workers):
    zombie = MockFxAsyncResult(state=STARTED, app=ut_app)
    succeeded = MockFxAsyncResult(state=SUCCESS, app=ut_app)
    ars = ManyFxAsyncResults.fx_ars_from_list([zombie, succeeded])

    with pytest.raises(ChainInterruptedByZombieTaskException):
        ars.wait_for_all(max_wait=30)
    assert always_dead_workers == [zombie] * 3
