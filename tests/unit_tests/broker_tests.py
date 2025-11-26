import time
import unittest

from firexkit.broker import handle_broker_timeout


def foo():
    return 1


def bar(a, b=1):
    return a+b


def fail():
    raise AssertionError()


def fail_with_timeout():
    raise TimeoutError()


class SucceedAfter():
    def __init__(self, succeed_after_retries):
        self.succeed_after_retries = succeed_after_retries
        self.tries = 0

    def foo(self):
        self.tries += 1
        if self.tries <= self.succeed_after_retries:
            raise TimeoutError()
        else:
            return


class HandleBrokerTimeoutTests(unittest.TestCase):

    def test_passing_callable(self):
        self.assertEqual(handle_broker_timeout(foo), 1)

    def test_passing_callable_with_args(self):
        self.assertEqual(handle_broker_timeout(bar, args=(1,)), 2)

    def test_passing_callable_with_kwargs(self):
        self.assertEqual(handle_broker_timeout(bar, kwargs={'a': 1}), 2)

    def test_passing_callable_with_args_and_kwargs(self):
        self.assertEqual(handle_broker_timeout(bar, args=(1,), kwargs={'b': 2}), 3)

    def test_fail(self):
        retry_delay = 1
        with self.assertRaises(AssertionError):
            start = time.time()
            handle_broker_timeout(fail, retry_delay=retry_delay)
        delta = time.time() - start
        # Make sure we didn't do sleep or retry
        self.assertLess(delta, retry_delay)

    def test_fail_with_timeout(self):
        timeout = 0.5
        retry_delay = 0.1
        with self.assertRaises(TimeoutError):
            start = time.time()
            handle_broker_timeout(fail_with_timeout, retry_delay=0.1, timeout=timeout)
        delta = time.time() - start
        self.assertGreater(delta, timeout)
        self.assertLess(delta, timeout+(retry_delay*10))  # handle_broker_timeout now uses exponential retry delay

    def test_succeed_after_retries(self):
        retry_delay = 0.1
        succeed_after_retries = 3
        obj = SucceedAfter(succeed_after_retries)
        start = time.time()
        self.assertIsNone(handle_broker_timeout(obj.foo, retry_delay=retry_delay))
        delta = time.time() - start
        self.assertGreater(delta, succeed_after_retries*retry_delay)
        self.assertLess(delta, (succeed_after_retries+1)*retry_delay)

    def test_success_not_reached_due_to_timeout(self):
        retry_delay = 0.1
        succeed_after_retries = 5
        timeout = 2*retry_delay
        obj = SucceedAfter(succeed_after_retries)
        start = time.time()
        with self.assertRaises(TimeoutError):
            handle_broker_timeout(obj.foo, retry_delay=retry_delay, timeout=timeout)
        delta = time.time() - start
        self.assertGreater(delta, timeout)
        self.assertLess(delta, timeout+retry_delay)
