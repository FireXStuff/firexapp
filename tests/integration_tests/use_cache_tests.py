import uuid
from celery.utils.log import get_task_logger

from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexkit.chain import returns
from firexkit.result import get_results
from firexapp.engine.celery import app
from firexkit.task import FireXTask

logger = get_task_logger(__name__)


def incr_and_concat(key_to_increment, a, b):
    app.backend.incr(key_to_increment)
    return a + b


@app.task(use_cache=True, returns='ab')
def Concat1(key_to_increment, a, b=''):
    return incr_and_concat(key_to_increment, a, b)


@app.task(use_cache=True)
@returns('ab')
def Concat2(key_to_increment, a, b=''):
    return incr_and_concat(key_to_increment, a, b)


@app.task(use_cache=True, returns=FireXTask.DYNAMIC_RETURN)
def Concat3(key_to_increment, a, b=''):
    return {'ab': incr_and_concat(key_to_increment, a, b)}


@app.task(use_cache=True)
@returns(FireXTask.DYNAMIC_RETURN)
def Concat4(key_to_increment, a, b=''):
    return {'ab': incr_and_concat(key_to_increment, a, b)}


def verify_caching_results(async_result, redis_key_to_check, return_value_expected, num_invocations_expected):
    return_value = get_results(async_result)
    assert return_value == return_value_expected, f'Expected {return_value_expected!r}, got {return_value!r}'
    num_invocations = int(app.backend.get(redis_key_to_check))
    assert num_invocations == num_invocations_expected, \
        f'Expected number of times to run {num_invocations_expected}, got {num_invocations}'


@app.task(bind=True)
def TestConcat(self):
    for task in [Concat1, Concat2, Concat3, Concat4]:
        logger.debug(f'Test case: {task.__name__} should get cached after first invocation unless use_cache was False')
        key_to_increment = str(uuid.uuid4())

        sig_with_use_cache_set_to_False = task.s()
        sig_with_use_cache_set_to_False.set_use_cache(False)

        c = task.s(key_to_increment=key_to_increment, a='a') | \
            task.s() | \
            task.s(b='') | \
            task.s(b='', a='a') | \
            sig_with_use_cache_set_to_False

        r = self.enqueue_child(c, block=True)
        return_value_expected = {'ab': 'a'}
        verify_caching_results(async_result=r,
                               redis_key_to_check=key_to_increment,
                               return_value_expected=return_value_expected,
                               num_invocations_expected=2)


@app.task(bind=True)
def TestConcatDoesNotGetForgotten(self):
    key_to_increment = str(uuid.uuid4())
    sig = Concat1.s(key_to_increment=key_to_increment, a='a')
    return_value_expected = {'ab': 'a'}

    # Call a cache-enabled service, and attempt to forget its result
    r1 = self.enqueue_child(sig, block=True, forget=True)
    # It's result should not be forgotten, since it's cache-enabled
    verify_caching_results(async_result=r1,
                           redis_key_to_check=key_to_increment,
                           return_value_expected=return_value_expected,
                           num_invocations_expected=1)
    # Call Concat1 again to make sure that we can retrieve the cached result
    r2 = self.enqueue_child(sig, block=True)
    verify_caching_results(async_result=r2,
                           redis_key_to_check=key_to_increment,
                           return_value_expected=return_value_expected,
                           num_invocations_expected=1)
    # but we should be okay to forget the second instance (r2)
    self.forget_specific_children_results([r2])
    assert r2.result is None


class TestCaching(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "TestConcat"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "Errors are not expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class TestCachingTaskDoesNotGetForgotten(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ['submit', '--chain', TestConcatDoesNotGetForgotten.name]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "Errors are not expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

@app.task(use_cache=True, returns='ret', flame='ret')
def Abc(arg):
    return arg

@app.task(bind=True)
def RunAbcConcurrently(self, num_runs=25):

    parallel_chains_1 = [Abc.s(1)]*num_runs
    parallel_chains_2 = [Abc.s(2)]*num_runs
    parallel_chains_3 = [Abc.s(3)]*num_runs

    results_1 = self.enqueue_in_parallel(parallel_chains_1, max_parallel_chains=25)
    results_2 = self.enqueue_in_parallel(parallel_chains_2, max_parallel_chains=25)
    results_3 = self.enqueue_in_parallel(parallel_chains_3, max_parallel_chains=25)

    self.wait_for_children()
    expected_return = 1
    for group in [results_1, results_2, results_3]:
        for r in group:
            returned = get_results(r)
            returned_val = returned.get('ret')
            logger.debug(f'Got {returned}')
            assert returned_val == expected_return, f'Should have returned {expected_return}, got {returned}'
        expected_return += 1

class TestCachingParallelInvocations(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ['submit', '--chain', RunAbcConcurrently.name]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "Errors are not expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)