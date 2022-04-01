from collections.abc import Iterable
from time import time
from functools import lru_cache

import celery.worker.state as state
from celery.worker.autoscale import Autoscaler
from celery.utils.log import get_task_logger
from firexkit.result import get_task_postrun_info

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.engine.logging import add_hostname_to_log_records, add_custom_log_levels, PRINT_LEVEL_NAME
from firexapp.discovery import find_firex_task_bundles
import billiard.pool
from billiard.five import values
from kombu.transport.redis import QoS


def _worker_active_monkey_patch(self, worker):
    for job in values(self._cache):
        worker_pids = job.worker_pids()
        # This crude fix would declare a worker busy if there were ANY jobs received but not ack'd
        # (i.e., were not assigned a worker pid yet)
        if not worker_pids or worker.pid in worker_pids:
            return True
    return False


# Monkey Patch for auto-scaler race condition where a forked worker pool instance that
# was sent a job (Pool.apply) but didn't get a chance to ack it (ApplyResult._ack)  would be wrongly
# eligible to be scaled down (Pool.shrink).
# This bug manifests itself in the following error:
# "Task handler raised error: WorkerLostError('Worker exited prematurely: signal 15 (SIGTERM) Job: 628.')"
billiard.pool.Pool._worker_active = _worker_active_monkey_patch
# End of Monkey Patch

# Monkey Patch: prevent tasks from running again if a worker receives SIGHUP
QoS.restore_at_shutdown = False


# End of Monkey Patch


# Below code needed to monkey-patch the autoscaler's qty() property
@lru_cache(maxsize=4096)
def _get_task_postrun_info(result, _call_time=None):
    # NOTE: _call_time arg is just used to invalidate the cache
    return get_task_postrun_info(result)


class _MemorizedTasksDone:
    def __init__(self, check_freq=63):
        self._check_freq = check_freq
        self._tasks_done = set()

    def tasks_done(self, results):
        call_time = int(time()) // self._check_freq
        if isinstance(results, (str, bytes)) or not isinstance(results, Iterable):
            results = [results]
        results = {str(result) for result in results}

        not_done = results - self._tasks_done
        done = {result for result in not_done if _get_task_postrun_info(result, call_time)}
        self._tasks_done.update(done)
        return frozenset(self._tasks_done)


_mtd = _MemorizedTasksDone()


# We need to make the autoscaler count revoked tasks if they are still running; otherwise
# it may never scale up if a revoked task schedules another task (from a finally: block, for example)
# Celery counts revoked tasks as DONE, and therefore it will not know these tasks are still runing
# and using a worker process
def _monkey_patch_autoscaler_qty(_self):
    return len(state.reserved_requests) + len(state.revoked) - len(_mtd.tasks_done(state.revoked))


setattr(Autoscaler, 'qty', property(_monkey_patch_autoscaler_qty))
# End monkey-patching qty()


add_custom_log_levels()
add_hostname_to_log_records()

# logging formats
timestamp_format = "<small>[%(asctime)s]"
process_format = "[%(levelname)s/%(processName)-13s]"
task_format = "[%(task_id).8s-%(task_name)s]"
message_format = ":</small> %(message)s"
worker_log_format = timestamp_format + process_format + message_format
worker_task_log_format = timestamp_format + process_format + task_format + message_format

logger = get_task_logger(__name__)

broker_url = BrokerFactory.get_broker_url()
result_backend = broker_url

# find default tasks
logger.debug("Beginning bundle discovery")
bundles = find_firex_task_bundles()
logger.debug("Bundle discovery completed.")
if bundles:
    logger.debug('Bundles discovered:\n' + '\n'.join([f'\t - {b}' for b in bundles]))

# Plugins are imported via firexapp.plugins._worker_init_signal()
imports = tuple(bundles) + tuple(["firexapp.tasks.example",
                                  "firexapp.tasks.core_tasks",
                                  "firexapp.tasks.root_tasks",
                                  "firexapp.submit.report_trigger",
                                  "firexapp.reporters.json_reporter"
                                  ])

root_task = "firexapp.tasks.root_tasks.RootTask"

accept_content = ['pickle', 'json']
task_serializer = 'pickle'
result_serializer = 'pickle'
result_expires = None

task_track_started = True
task_acks_late = True

worker_prefetch_multiplier = 1
worker_redirect_stdouts_level = PRINT_LEVEL_NAME

primary_worker_name = 'mc'
primary_worker_minimum_concurrency = 4
mc = BrokerFactory.get_hostname_port_from_url(broker_url)[0]
