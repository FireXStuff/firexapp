import unittest
from celery import Celery
from celery.result import AsyncResult
from celery.states import SUCCESS, FAILURE, REVOKED, STARTED, PENDING
from contextlib import contextmanager
from firexkit.result import wait_on_async_results, get_task_name_from_result, get_result_logging_name, \
    is_result_ready, WaitLoopCallBack, WaitOnChainTimeoutError, ChainRevokedException, ChainInterruptedException, \
    get_tasks_names_from_results, MultipleFailuresException, find_unsuccessful_in_chain, \
    monkey_patch_async_result_to_track_instances, teardown_monkey_patch_async_result_to_track_instances, \
    last_causing_chain_interrupted_exception, first_non_chain_interrupted_exception
from firexkit.revoke import RevokedRequests


class MockResult(AsyncResult):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._state = None
        self._result = None
        self._kids = []

    @classmethod
    def set_heritage(cls, parent, child):
        child.parent = parent
        parent.children += [child]

    @property
    def state(self):
        if callable(self._state):
            return self._state()
        return self._state


    @state.setter
    def state(self, state):
        self._state = state

    @property
    def result(self):
        if callable(self._result):
            return self._result()
        return self._result


    @result.setter
    def result(self, result):
        self._result = result

    @property
    def children(self):
        return self._kids

    @children.setter
    def children(self, kids):
        self._kids = kids


def get_mocks(result_ids=None):
    if result_ids is None:
        result_ids = ["anything"]

    test_app = Celery()
    test_app.config_from_object({
        "result_backend": 'cache',
        "cache_backend": 'memory'
    })
    mock_results = [MockResult(id=r, app=test_app) for r in result_ids]
    if len(mock_results) == 1:
        return test_app, mock_results[0]
    return test_app, mock_results


def setup_revoke(revoked=tuple()):
    revokes = type('NotRevokedRequests', (object,), {})()
    revokes.is_revoked = lambda result_id: result_id in revoked
    RevokedRequests.instance(revokes)


class ResultsLoggingNamesTests(unittest.TestCase):
    def test_get_task_name(self):
        result_id = "anything"
        test_app, mock_result = get_mocks([result_id])

        found_name = get_task_name_from_result(mock_result)
        self.assertEqual(found_name, "")

        test_app.backend.set(result_id, "yes".encode('utf-8'))
        found_name = get_task_name_from_result(mock_result)
        self.assertEqual(found_name, "yes")

    def test_get_many_task_names(self):
        test_app, mock_results = get_mocks(["a", "b"])
        test_app.backend.set("a", "yes".encode('utf-8'))
        test_app.backend.set("b", "yes".encode('utf-8'))
        found_name = get_tasks_names_from_results(mock_results)
        self.assertEqual(found_name, ["yes[a]", "yes[b]"])

    def test_get_logging_name(self):
        test_app, mock_result = get_mocks()

        log_name = get_result_logging_name(mock_result, name="yes")
        self.assertEqual(log_name, "yes[anything]")

        log_name = get_result_logging_name(mock_result)
        self.assertEqual(log_name, "[anything]")


class ResultsReadyTests(unittest.TestCase):

    def test_is_ready(self):
        test_app, mock_result = get_mocks()
        mock_result.state = SUCCESS
        self.assertTrue(is_result_ready(mock_result))

        mock_result.state = FAILURE
        self.assertTrue(is_result_ready(mock_result))

        mock_result.state = REVOKED
        self.assertTrue(is_result_ready(mock_result))

        mock_result.state = STARTED
        self.assertFalse(is_result_ready(mock_result))

    def test_backend_exception(self):
        test_app, mock_result = get_mocks()

        # exceptions go up the stack
        def bad_backend():
            raise AssertionError()
        mock_result.state = bad_backend
        with self.assertRaises(AssertionError):
            is_result_ready(mock_result)

        # exceptions go up the stack even if trials are enabled
        def bad_backend():
            mock_result.state = SUCCESS
            raise AssertionError()
        mock_result.state = bad_backend
        with self.assertRaises(AssertionError):
            is_result_ready(mock_result, timeout=5)

        # Timeouts try again
        def bad_backend():
            mock_result.state = SUCCESS
            raise TimeoutError()
        mock_result.state = bad_backend
        self.assertTrue(is_result_ready(mock_result, retry_delay=0))

        # Timeouts try again only a few times
        def bad_backend():
            raise TimeoutError()
        mock_result.state = bad_backend
        with self.assertRaises(TimeoutError):
            is_result_ready(mock_result, timeout=3, retry_delay=0)


class WaitOnResultsTests(unittest.TestCase):
    def test_wait_on_nothing(self):
        # make sure the function returns, although in doesn't return anything
        self.assertIsNone(wait_on_async_results(results=None))

    def test_wait_on_single_result(self):
        setup_revoke()
        test_app, mock_result = get_mocks()
        test_app.backend.set("anything", "yep".encode('utf-8'))

        mock_result.state = SUCCESS
        self.assertIsNone(wait_on_async_results(results=mock_result))

        # wait then go
        def wait_and_go():
            mock_result.state = SUCCESS
            return STARTED
        mock_result.state = wait_and_go
        try:
            self.assertIsNone(wait_on_async_results(results=mock_result))
        finally:
            mock_result.backend = None

    @contextmanager
    def prime_mocks(self, mock_results, expected_hits):
        hits = []
        for r in mock_results:
            def wait_and_go(r1=r):
                def started_and_go(r2=r1):
                    r2.state = SUCCESS
                    hits.append(r2)
                    return STARTED
                r1.state = started_and_go
                return PENDING

            r.state = wait_and_go
        yield
        self.assertEqual(len(hits), expected_hits)

    def test_wait_on_many_results(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a1", "a2", "a3"])

        with self.prime_mocks(mock_results, 3):
            self.assertIsNone(wait_on_async_results(results=mock_results))

    def test_wait_on_chain(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockResult.set_heritage(mock_results[1], mock_results[2])
        MockResult.set_heritage(mock_results[0], mock_results[1])

        with self.prime_mocks(mock_results, 3):
            self.assertIsNone(wait_on_async_results(results=mock_results[2]))

        unsuccessful = find_unsuccessful_in_chain(mock_results[-1])
        self.assertDictEqual(unsuccessful, {})

    def test_self_parent_recursion(self):
        setup_revoke()
        test_app, mock_result = get_mocks()
        MockResult.set_heritage(mock_result, mock_result)
        mock_result.state = SUCCESS
        self.assertIsNone(wait_on_async_results(results=mock_result))

    def test_callbacks(self):
        setup_revoke()
        test_app, mock_result = get_mocks()
        mock_result.state = STARTED

        def call_this():
            call_this.was_called += 1
            if call_this.was_called == 5:
                mock_result.state = SUCCESS

        call_this.was_called = 0

        callbacks = [WaitLoopCallBack(func=call_this, frequency=0.2, kwargs={})]
        self.assertIsNone(wait_on_async_results(results=mock_result, callbacks=callbacks))
        self.assertEqual(call_this.was_called, 5)

    def test_Chain_interrupted(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockResult.set_heritage(mock_results[1], mock_results[2])
        MockResult.set_heritage(mock_results[0], mock_results[1])
        mock_results[0].state = SUCCESS
        mock_results[1].state = FAILURE
        mock_results[2].state = PENDING
        with self.assertRaises(ChainInterruptedException) as context:
            wait_on_async_results(results=mock_results[2])
        self.assertIsNone(context.exception.__cause__)

        unsuccessful = find_unsuccessful_in_chain(mock_results[-1])
        self.assertDictEqual(unsuccessful, {'not_run': [mock_results[2]], 'failed': [mock_results[1]]})

    def test_Chain_interrupted_from_exc(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1"])
        MockResult.set_heritage(mock_results[0], mock_results[1])
        mock_results[0].state = SUCCESS
        mock_results[1].state = FAILURE
        mock_results[1].result = OSError()
        with self.assertRaises(ChainInterruptedException) as context:
            wait_on_async_results(results=mock_results[1])
        self.assertTrue(isinstance(context.exception.__cause__, OSError))

    def test_timeout(self):
        setup_revoke()
        test_app, mock_result = get_mocks()
        mock_result.state = STARTED
        with self.assertRaises(WaitOnChainTimeoutError):
            wait_on_async_results(results=mock_result, max_wait=0.2)

    def test_wait_on_revoked_chain(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])
        MockResult.set_heritage(mock_results[1], mock_results[2])
        MockResult.set_heritage(mock_results[0], mock_results[1])

        for i in range(3):
            # middle of the chain is revoked
            mock_results[0].state = SUCCESS
            mock_results[1].state = SUCCESS
            mock_results[2].state = STARTED

            mock_results[i].state = REVOKED
            with self.assertRaises(ChainRevokedException):
                wait_on_async_results(results=mock_results[2])

    def test_wait_on_revoked_result(self):
        setup_revoke(["rev"])
        test_app, mock_result = get_mocks(["rev"])
        mock_result.state = PENDING
        with self.assertRaises(ChainRevokedException):
            wait_on_async_results(results=mock_result)

        unsuccessful = find_unsuccessful_in_chain(mock_result)
        self.assertDictEqual(unsuccessful, {'not_run': [mock_result]})

    def test_wait_for_all_even_on_failure(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2"])

        with self.prime_mocks(mock_results, 2):
            # a0 and a2 should both be hit, but not a1
            mock_results[1].state = FAILURE
            mock_results[1].results = OSError()
            with self.assertRaises(ChainInterruptedException):
                wait_on_async_results(results=mock_results)

    def test_multiple_failures(self):
        setup_revoke()
        test_app, mock_results = get_mocks(["a0", "a1", "a2", "a3"])

        with self.prime_mocks(mock_results, 1):
            # a0 and a1 should both have failure
            mock_results[1].state = FAILURE
            mock_results[1].results = OSError()
            mock_results[2].state = PENDING
            mock_results[3].state = FAILURE
            mock_results[3].results = NotImplementedError()

            # make a1-a2 a chain, so a2 will not be hit
            MockResult.set_heritage(mock_results[1], mock_results[2])
            mock_results.remove(mock_results[1])

            with self.assertRaises(MultipleFailuresException) as multi_failure:
                self.assertIsNone(wait_on_async_results(results=mock_results))
            multi_failure_exception = multi_failure.exception
            self.assertEqual(len(multi_failure_exception.failures), 2)
            self.assertTrue(isinstance(multi_failure_exception.failures[0], ChainInterruptedException))
            self.assertTrue(isinstance(multi_failure_exception.failures[1], ChainInterruptedException))


class TrackInstancesTests(unittest.TestCase):

    def setUp(self):
        teardown_monkey_patch_async_result_to_track_instances()

    def tearDown(self):
        teardown_monkey_patch_async_result_to_track_instances()

    def test_monkey_patch_track(self):
        monkey_patch_async_result_to_track_instances()
        ar1 = AsyncResult(id='1')
        ar2 = AsyncResult(id='2')
        instances = list(AsyncResult.get_ar_instances())
        self.assertEqual([ar1, ar2], instances)

    def test_fail_double_monkey_patch_track(self):
        monkey_patch_async_result_to_track_instances()
        self.assertRaises(AssertionError, monkey_patch_async_result_to_track_instances)


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

