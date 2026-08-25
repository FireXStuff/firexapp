import unittest
from unittest import mock
from celery.states import SUCCESS, FAILURE, REVOKED, STARTED, PENDING
from contextlib import contextmanager
from firexkit.result import wait_on_async_results, \
    WaitLoopCallBack, WaitOnChainTimeoutError, ChainRevokedException, ChainInterruptedException, \
    MultipleFailuresException, find_unsuccessful_in_chain, \
    last_causing_chain_interrupted_exception, first_non_chain_interrupted_exception
from firexkit.revoke import RevokedRequests
from firexkit.testing import MockFxAsyncResult
from firexkit.firex_celery import FireXCelery


def get_mocks(
    result_ids=None,
    _state=None,
) -> tuple[FireXCelery, list[MockFxAsyncResult]]:
    if result_ids is None:
        result_ids = ["anything"]

    test_app = FireXCelery()
    test_app.config_from_object({
        "result_backend": 'cache',
        "cache_backend": 'memory'
    })
    mock_results = [
        MockFxAsyncResult(state=_state, id=r, app=test_app)
        for r in result_ids
    ]
    return test_app, mock_results


def setup_revoke(revoked=tuple()):
    revokes = type('NotRevokedRequests', (object,), {})()
    revokes.is_revoked = lambda result_id: result_id in revoked
    RevokedRequests.instance(revokes)


class ResultsLoggingNamesTests(unittest.TestCase):
    def test_get_task_name(self):
        result_id = "anything"
        test_app, mock_result = get_mocks([result_id])
        mock_result = mock_result[0]
        mock_result._name = None

        with mock.patch.object(test_app.backend.client, 'hget', create=True, return_value=b'yes'):
            self.assertEqual(mock_result.fx_get_name(), "yes")

    def test_get_logging_name(self):
        test_app, mock_result = get_mocks()
        self.assertEqual(mock_result[0].fx_logging_name(), "[anything]")


class ResultsReadyTests(unittest.TestCase):

    def test_is_ready(self):
        test_app, mock_result = get_mocks()
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
        test_app, mock_result = get_mocks()
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
        test_app, mock_result = get_mocks()
        mock_result = mock_result[0]
        test_app.backend.set("anything", "yep".encode('utf-8'))

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
        test_app, mock_results = get_mocks(["a1", "a2", "a3"])

        with self.prime_mocks(mock_results, 3):
            self.assertIsNone(wait_on_async_results(mock_results))

    def test_wait_on_chain(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockFxAsyncResult.set_heritage(mock_results[1], mock_results[2])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])

        with self.prime_mocks(mock_results, 3):
            self.assertIsNone(wait_on_async_results(mock_results[2]))

        unsuccessful = find_unsuccessful_in_chain(mock_results[-1])
        self.assertDictEqual(unsuccessful, {})

    def test_self_parent_recursion(self):
        setup_revoke()
        test_app, mock_result = get_mocks(_state=SUCCESS)
        mock_result = mock_result[0]
        MockFxAsyncResult.set_heritage(mock_result, mock_result)
        self.assertIsNone(wait_on_async_results(mock_result))

    def test_callbacks(self):
        setup_revoke()
        test_app, mock_result = get_mocks()
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
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockFxAsyncResult.set_heritage(mock_results[1], mock_results[2])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])
        mock_results[0]._state = SUCCESS
        mock_results[1]._state = FAILURE
        mock_results[2]._state = PENDING
        with self.assertRaises(ChainInterruptedException) as context:
            print(f'will wait on ar')
            wait_on_async_results(mock_results[2], max_wait=1)
            print(f'done wait on ar')
        self.assertIsNone(context.exception.__cause__)

        unsuccessful = find_unsuccessful_in_chain(mock_results[-1])
        self.assertDictEqual(unsuccessful, {'not_run': [mock_results[2]], 'failed': [mock_results[1]]})

    def test_Chain_interrupted_from_exc(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1"])
        MockFxAsyncResult.set_heritage(mock_results[0], mock_results[1])
        mock_results[0]._state = SUCCESS
        mock_results[1]._state = FAILURE
        mock_results[1]._result = OSError()
        with self.assertRaises(ChainInterruptedException) as context:
            wait_on_async_results(mock_results[1])
        self.assertTrue(isinstance(context.exception.__cause__, OSError))

    def test_timeout(self):
        setup_revoke()
        test_app, mock_result = get_mocks()
        mock_result = mock_result[0]
        mock_result._state = STARTED
        with self.assertRaises(WaitOnChainTimeoutError):
            wait_on_async_results(mock_result, max_wait=0.2)

    def test_wait_on_revoked_chain(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])
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
        setup_revoke(["rev"])
        test_app, mock_result = get_mocks(["rev"])
        mock_result = mock_result[0]
        mock_result._state = PENDING
        with self.assertRaises(ChainRevokedException):
            wait_on_async_results(mock_result)

        unsuccessful = find_unsuccessful_in_chain(mock_result)
        self.assertDictEqual(unsuccessful, {'not_run': [mock_result]})

    def test_wait_for_all_even_on_failure(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])

        with self.prime_mocks(mock_results, 2):
            # a0 and a2 should both be hit, but not a1
            mock_results[1]._state = FAILURE
            mock_results[1]._result = OSError()
            with self.assertRaises(ChainInterruptedException):
                wait_on_async_results(mock_results)

    def test_multiple_failures(self):
        setup_revoke()
        mock_results = get_mocks(["a0", "a1", "a2", "a3"])[1]

        print(f'Initial state of first: {mock_results[0].state}')
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
        self.assertTrue(isinstance(multi_failure_exception.failures[0], ChainInterruptedException))
        self.assertTrue(isinstance(multi_failure_exception.failures[1], ChainInterruptedException))


class WalkExceptionTests(unittest.TestCase):

    def test_last_chain_interrupted(self):

        e1 = Exception('exception1')
        try:
            raise ChainInterruptedException('exception2') from e1
        except ChainInterruptedException as e:
            e2 = e

        try:
            raise ChainInterruptedException('exception3') from e2
        except ChainInterruptedException as e:
            e3 = e

        try:
            raise ChainInterruptedException('exception3') from e3
        except ChainInterruptedException as e:
            e4 = e

        last_cause = last_causing_chain_interrupted_exception(e4)
        self.assertIs(e2, last_cause)

    def test_fail_double_monkey_patch_track(self):
        e1 = Exception('exception1')
        try:
            raise ChainInterruptedException('exception2') from e1
        except ChainInterruptedException as e:
            e2 = e

        try:
            raise ChainInterruptedException('exception3') from e2
        except ChainInterruptedException as e:
            e3 = e

        non_chain_interrupted = first_non_chain_interrupted_exception(e3)
        self.assertIs(e1, non_chain_interrupted)

