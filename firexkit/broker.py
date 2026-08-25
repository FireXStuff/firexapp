import time
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


def handle_broker_timeout(callable_func, args=(), kwargs=None, timeout=15*60, retry_delay=1, reraise_on_timeout=True):
    if kwargs is None:
        kwargs = {}
    maximum_retry_delay = retry_delay * 10
    timeout_time = time.monotonic() + timeout if timeout else None
    tries = 0

    while True:
        tries += 1
        func_start_time = time.monotonic()
        try:
            return_value = callable_func(*args, **kwargs)
        except Exception as e:
            # need to handle different exceptions from different brokers
            e_name = type(e).__name__
            if e_name != "TimeoutError" and e_name != "ConnectionError":
                raise

            current_time = time.monotonic()
            if timeout_time is not None and current_time >= timeout_time:

                logger.error(f'Reached max timeout of {timeout}s...giving up '
                             f'(last call took {(current_time - func_start_time) * 1000:.2f}ms)')
                try:
                    send_task_instrumentation_event(instrumentation_label='handle_broker_timeout-failure',
                                                    broker_timeout_tries=tries)
                except Exception as e:
                    logger.debug('Cannot send instrumentation event', exc_info=e)

                if reraise_on_timeout:
                    # Raise the initial callable_func() error
                    raise
                return None

            logger.warning(f'Backend was not reachable... '
                           f' retrying in {retry_delay}s '
                           f'(last call took {(current_time - func_start_time) * 1000:.2f}ms)')
            if timeout_time is not None:
                logger.warning(f'Final timeout in {timeout_time - current_time}s')

            time.sleep(retry_delay)
            # Exponential backoff
            retry_delay = retry_delay * 1.1 \
                if retry_delay * 1.1 < maximum_retry_delay else maximum_retry_delay

        else:
            # callable_func() returned successfully
            if tries > 1:
                try:
                    send_task_instrumentation_event(instrumentation_label='handle_broker_timeout-success',
                                                    broker_timeout_tries=tries)
                except Exception as e:
                    logger.debug('Cannot send instrumentation event', exc_info=e)

            return return_value


def send_task_instrumentation_event(event_type='task-instrumentation', **fields):
    from celery import current_task
    if not current_task:
        return
    try:
        import traceback
        current_task.send_event(event_type, instrumentation_event_stack=traceback.format_stack(), **fields)
    except Exception as e:
        logger.debug(e, exc_info=True)
        logger.debug('Could not instrument')