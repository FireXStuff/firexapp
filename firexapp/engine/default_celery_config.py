from collections.abc import Iterable
from time import time
from functools import lru_cache
import dataclasses
import typing
import os

import celery.worker.state as state
from celery.worker.autoscale import Autoscaler
from celery.utils.log import get_task_logger
from firexkit.result import get_task_postrun_info

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.engine.logging import add_hostname_to_log_records, add_custom_log_levels, PRINT_LEVEL_NAME
from firexapp.submit.uid import Uid
from firexapp.discovery import find_firex_task_bundles
import billiard.pool
from kombu.transport.redis import QoS


def _worker_active_monkey_patch(self, worker):
    for job in self._cache.values():
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

logger = get_task_logger(__name__)


@dataclasses.dataclass
class FireXAppCeleryConfig:

    # logging formats
    timestamp_format: str = "<small>[%(asctime)s]"
    process_format: str = "[%(levelname)s/%(processName)-13s]"
    task_format: str = "[%(task_id).8s-%(task_name)s]"
    message_format: str = ":</small> %(message)s"
    worker_log_format: str = timestamp_format + process_format + message_format
    worker_task_log_format: str = timestamp_format + process_format + task_format + message_format

    broker_connection_retry_on_startup: bool = True

    imports: typing.Optional[tuple[str, ...]] = None

    root_task: str = "firexapp.tasks.root_tasks.RootTask"

    accept_content: tuple[str, ...] = ('pickle', 'json')
    task_serializer: str = 'pickle'
    result_serializer: str = 'pickle'
    result_expires: typing.Any = None

    task_track_started: bool = True
    task_acks_late: bool = True

    worker_prefetch_multiplier: int = 1
    worker_redirect_stdouts_level: str = PRINT_LEVEL_NAME

    # broker attributes set by __post_init__
    broker_url: str = ''
    result_backend: str = ''
    mc = '' # mc hostname

    task_default_queue: str = 'mc'
    uid: str = ''
    logs_dir: str = ''

    static_imports : tuple[str, ...] = (
        "firexapp.tasks.example",
        "firexapp.tasks.core_tasks",
        "firexapp.tasks.root_tasks",
        "firexapp.submit.report_trigger",
        "firexapp.reporters.json_reporter",
    )
    extra_imports : tuple[str, ...] = tuple()

    fx_discover_tasks : bool = True

    def __post_init__(self):
        self.set_run_var_from_env()
        self.set_imports()

    def set_run_var_from_env(self):
        # logically should be able to assert, but UT
        # does auto-initialising imports.
        run_env = RunEnvVars.get_from_env()
        self.uid : str = run_env.firex_id
        self.logs_dir: str = run_env.firex_logs_dir
        self.broker_url = run_env.broker_url
        self.result_backend = run_env.broker_url
        self.mc = BrokerFactory.get_hostname_port_from_url(run_env.broker_url)[0]


    def _discover_task_bundles(self) -> list[str]:
        return [] #find_firex_task_bundles() #find_firex_task_bundles()

    def set_imports(self):
        # find default tasks
        if self.fx_discover_tasks:
            logger.debug("Beginning bundle discovery")
            bundles = self._discover_task_bundles()
            logger.debug("Bundle discovery completed.")
            if bundles:
                logger.debug('Bundles discovered:\n' + '\n'.join([f'\t - {b}' for b in bundles]))
        else:
            bundles = []
        self.imports = tuple(bundles) + self.static_imports + self.extra_imports


@dataclasses.dataclass
class RunEnvVars:
    firex_id: str
    firex_logs_dir: str
    broker_url: str
    # redis_bin_dir: str

    @classmethod
    def get_from_env(cls) -> 'RunEnvVars':
        return RunEnvVars(
            firex_id=os.environ.get('CURRENT_RUN_FIREX_ID', ''),
            firex_logs_dir=os.environ.get('firex_logs_dir', ''),
            broker_url=os.environ.get(BrokerFactory.broker_env_variable, ''),
        )

    @classmethod
    def set_firex_environ(
        cls,
        broker_url: str,
        uid: typing.Optional[Uid]=None,
        logs_dir: typing.Optional[str]=None,
    ) -> 'RunEnvVars':
        assert uid or logs_dir, 'Must supply uid or logs_dir'
        os.environ.update({
            'CURRENT_RUN_FIREX_ID': str(uid) if uid else os.path.basename(logs_dir),
            'firex_logs_dir': logs_dir or uid.logs_dir,
            BrokerFactory.broker_env_variable: broker_url,
        })
        return cls.get_from_env()

    @staticmethod
    def env_var_names() -> list[str]:
        return ['CURRENT_RUN_FIREX_ID', 'firex_logs_dir', BrokerFactory.broker_env_variable]


MC_MIN_CONCURRENCY = 4
