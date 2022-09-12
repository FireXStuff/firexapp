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


class TestCaching(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "TestConcat"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, "Errors are not expected"

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
