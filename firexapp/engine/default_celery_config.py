import dataclasses
import os
from typing import Annotated, Any, ClassVar

import billiard.pool
import pydantic
from celery.utils.log import get_task_logger
from kombu.transport.redis import QoS
from typing_extensions import Self

import firexapp.discovery
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.broker_manager.redis_manager import RedisManager
from firexapp.engine.autoscaler import FireXAutoscaler
from firexapp.engine.logging import (
    PRINT_LEVEL_NAME,
    add_custom_log_levels,
    add_hostname_to_log_records,
)
from firexapp.submit.install_configs import (
    install_config_path_from_logs_dir,
    load_existing_install_configs,
)
from firexapp.submit.uid import is_firex_id


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

# prevent tasks from running again if a worker receives SIGHUP
QoS.restore_at_shutdown = False

add_custom_log_levels()
add_hostname_to_log_records()

logger = get_task_logger(__name__)

def _plugins_to_csv(plugins: Any):
    if isinstance(plugins, list):
        return ','.join([
            p for p in plugins if p
        ])
    return plugins or ''


class FxEnvVars(pydantic.BaseModel):
    # field names are ENV key names.
    CURRENT_RUN_FIREX_ID: str
    firex_logs_dir: str
    redis_bin_dir: str
    BROKER: str # url
    firex_plugins: Annotated[
        str,
        pydantic.BeforeValidator(_plugins_to_csv),
    ] = ''

    @property
    def firex_id(self) -> str:
        return self.CURRENT_RUN_FIREX_ID

    @property
    def logs_dir(self) -> str:
        return self.firex_logs_dir

    @property
    def broker_url(self) -> str:
        return self.BROKER

    def get_plugin_files(self) -> list[str]:
        return [
            p for p in self.firex_plugins.split(',') if p
        ]

    def get_redis_hostname(self) -> str:
        return RedisManager.get_hostname_port_from_url(self.BROKER)[0]

    def set_fx_os_env(self):
        os.environ.update(
            self.model_dump(),
        )

    @classmethod
    def clear_os_fx_env(cls):
        for k in FxEnvVars.model_fields:
            # redis bin dir not run specific.
            if k != 'redis_bin_dir':
                cleared_val = os.environ.pop(k, None)
                if cleared_val:
                    logger.info(f'Cleared environment of {k}={cleared_val}')

    @classmethod
    def load_firex_env_vars_from_env(cls) -> Self:
        fx_env = cls.model_validate(os.environ)

        assert fx_env.CURRENT_RUN_FIREX_ID, "loading a FireX env must include a CURRENT_RUN_FIREX_ID"
        assert is_firex_id(fx_env.CURRENT_RUN_FIREX_ID), f'CURRENT_RUN_FIREX_ID env is not a FireX ID: {fx_env.CURRENT_RUN_FIREX_ID}'
        assert fx_env.firex_logs_dir, "loading a FireX env must include a firex_logs_dir"
        assert fx_env.BROKER, "loading a FireX env must include a BROKER"

        return fx_env

    @classmethod
    def load_firex_env_vars_from_logs_dir(
        cls,
        logs_dir: str,
        plugins,
    ) -> Self:
        broker_mgr = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
        firex_id = os.path.basename(logs_dir)
        if not is_firex_id(firex_id):
            logger.error(f'Basename {firex_id} of log directory {logs_dir} is not a FireX ID')
        return cls.model_validate(
            dict(
                CURRENT_RUN_FIREX_ID=firex_id,
                firex_logs_dir=logs_dir,
                redis_bin_dir=broker_mgr.redis_bin_base,
                BROKER=broker_mgr.broker_url,
                firex_plugins=plugins,
            )
        )

    @classmethod
    def create_no_task_exec_fx_env(
        cls,
        plugins=None,
        logs_dir='',
    ) -> Self:
        return cls.model_validate(
            dict(
                CURRENT_RUN_FIREX_ID='',
                firex_logs_dir=logs_dir,
                redis_bin_dir='',
                BROKER='',
                firex_plugins=plugins,
            )
        )

    @classmethod
    def select_minimal_fx_env_from_os_env(cls) -> dict[str, str]:
        env_names = ['PATH', 'PYTHONPATH', 'VIRTUAL_ENV'] + list(cls.model_fields)
        return {
            k: v for k, v in os.environ.items()
            if k in env_names
        }


# Modules that must be imported by every FireX app, independently of bundle
# discovery, because importing them has registration side-effects that the run
# depends on (e.g. firexapp.submit.report_trigger registers the root-task
# converter that triggers pre-run reports, firexapp.tasks.root_tasks registers
# the root-task postrun completion handler).
# Apps that override FxCeleryConfig.imports to skip bundle discovery must still
# include these.
FIREX_APP_ROOT_TASK_MODULE = "firexapp.tasks.root_tasks"
FIREXAPP_INFRA_IMPORTS = (
    "firexapp.tasks.core_tasks",
    "firexapp.submit.report_trigger",
    "firexapp.reporters.json_reporter",
    "firex_bundle_ci.tasks"
)


@dataclasses.dataclass
class FxCeleryConfig:

    fx_env: FxEnvVars

    # logging formats
    timestamp_format: str = "<small>[%(asctime)s]"
    process_format: str = "[%(levelname)s/%(processName)-13s]"
    task_format: str = "[%(task_id).8s-%(task_name)s]"
    message_format: str = ":</small> %(message)s"
    worker_log_format: str = timestamp_format + process_format + message_format
    worker_task_log_format: str = timestamp_format + process_format + task_format + message_format

    broker_connection_retry_on_startup: bool = True

    root_task = f"{FIREX_APP_ROOT_TASK_MODULE}.RootTask"
    worker_autoscaler = FireXAutoscaler

    accept_content = ['pickle', 'json']
    task_serializer = 'pickle'
    result_serializer = 'pickle'
    result_expires = None

    task_track_started = True
    task_acks_late = True

    worker_prefetch_multiplier: int = 1
    worker_redirect_stdouts_level: str = PRINT_LEVEL_NAME

    task_default_queue = 'mc'
    primary_worker_name = 'mc'

    task_soft_time_limit : int = 72 * 60

    primary_worker_minimum_concurrency : ClassVar[int] = 4

    link_for_logo : str | None = None
    logs_url : str | None = None
    resources_dir : str | None = None

    def __post_init__(self):
        if not self.link_for_logo:
            self.link_for_logo = self.logs_dir

    @property
    def broker_url(self) -> str:
        return self.fx_env.broker_url

    @property
    def result_backend(self) -> str:
        return self.fx_env.broker_url

    @property
    def broker(self) -> str:
        return self.fx_env.broker_url

    @property
    def mc(self) -> str:
        return self.fx_env.get_redis_hostname()

    @property
    def redis_bin_dir(self) -> str:
        return self.fx_env.redis_bin_dir

    @property
    def uid(self) -> str:
        return self.fx_env.firex_id

    @property
    def logs_dir(self) -> str:
        return self.fx_env.logs_dir

    def load_install_config(self):
        install_config = install_config_path_from_logs_dir(self.logs_dir)
        assert os.path.isfile(install_config), (
            f"Install config missing from run, firexapp submit is expected to have populated it: {install_config}"
        )

        # TODO: assumes everywhere celery is started can load from logs_dir. Should likely serialize to backend.
        self.install_config = load_existing_install_configs(self.fx_env.firex_id, self.logs_dir)
        if self.install_config.has_viewer():
            self.logs_url = self.link_for_logo = self.install_config.get_logs_root_url()
            self.link_for_logo = self.install_config.get_logs_root_url()

    @classmethod
    def _fx_discover_bundles(cls):
        # find default tasks
        logger.debug("Beginning bundle discovery")
        bundles = firexapp.discovery.find_firex_task_bundles()
        logger.debug("Bundle discovery completed.")
        if bundles:
            logger.debug('Bundles discovered:\n' + '\n'.join([f'\t - {b}' for b in bundles]))
        return bundles

    @property
    def imports(self) -> tuple[str, ...]:
        return tuple(
            self._fx_discover_bundles()
        ) + (
            FIREX_APP_ROOT_TASK_MODULE,
            "firexapp.tasks.example",
        ) + FIREXAPP_INFRA_IMPORTS
