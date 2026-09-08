import importlib
import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import ClassVar

import celery.signals
from celery.app import app_or_default
from celery.app.base import Celery
from celery.backends.redis import RedisBackend
from celery.exceptions import NotRegistered
from celery.loaders.base import BaseLoader
from celery.utils.log import get_task_logger
from celery.worker.components import Hub
from celery.worker.consumer import Consumer
from kombu.utils.objects import cached_property
from typing_extensions import Self

import firexkit.broker
from firexapp.broker_manager.broker_factory import BrokerFactory, RedisManager
from firexapp.celery_manager import CeleryManager
from firexapp.engine.default_celery_config import FxCeleryConfig, FxEnvVars
from firexapp.engine.firex_revoke import RevokeDetails
from firexapp.events.model import RunStates
from firexapp.plugins import (
    FxPluginRegistry,
    convert_plugins_to_list,
)
from firexapp.reporters.json_reporter import ReporterStep
from firexapp.submit.uid import Uid
from firexkit.task import FireXTask, convert_to_serializable

logger = get_task_logger(__name__)

_TASK_PRE_RUN_KEY = 'TASK_PRE_RUN'
_TASK_POST_RUN_KEY = 'TASK_POST_RUN'

class FireXCelery(Celery):

    _GLOBAL_APP_PKG : ClassVar[str] = 'firexapp.engine.celery'

    def __init__(
        self,
        *args,
        fx_env: FxEnvVars,
        task_cls='firexkit.task:FireXTask',
        fx_celery_config: FxCeleryConfig | None=None,
        fx_plugins_reg=FxPluginRegistry(),
        fx_expect_tasks=True,
        strict_typing=False,
        **kwargs,
    ):
        self._fx_task_execution_wired = False
        super().__init__(
            *args,
            task_cls=task_cls,
            strict_typing=strict_typing,
            **kwargs,
        )
        self._apply_fx_config(
            fx_env=fx_env,
            fx_celery_config=fx_celery_config,
            fx_plugins_reg=fx_plugins_reg,
            fx_expect_tasks=fx_expect_tasks,
        )

    @classmethod
    def set_worker_fx_app(cls) -> Self:
        """
            Creates an app that is not the submit app,
            and therefore expects all infra preconditions
            (e.g. redis, celery starting, firex_id and
             other metadata in backend, etc)
            have already succeeded
        """
        CeleryManager.unset_start_fx_celery_worker_env()
        existing_fx_app = cls._get_global_fx_app()
        fx_env = FxEnvVars.load_firex_env_vars_from_env()
        if existing_fx_app and existing_fx_app._fx_task_execution_wired:
            fx_app = existing_fx_app
            if not fx_env.firex_id != ( app_fid := fx_app.conf.fx_env.firex_id ):
                logger.error(f'Found existing fx_app {app_fid} not equal to current environment run ID {fx_env.firex_id}')
        else:
            fx_app = cls._get_promotable_app()
            if fx_app is not None:
                fx_app._apply_fx_config(fx_env=fx_env)
            else:
                fx_app = cls(fx_env=fx_env)
            cls._set_global_fx_app(fx_app)
            # set OS ENV vars for this run.
            fx_app.conf.fx_env.set_fx_os_env()
            fx_app.load_backend_conf_fields()
            fx_app.import_microservices()

        return fx_app

    def _apply_fx_config(
        self,
        *,
        fx_env: FxEnvVars,
        fx_celery_config: FxCeleryConfig | None=None,
        fx_plugins_reg: FxPluginRegistry | None=None,
        fx_expect_tasks: bool = True,
    ) -> None:
        """
            Applies (or re-applies) FireX-specific configuration to this app
            instance. Split out from __init__ so that promoting an already-
            constructed app (create_submit_fx_app/set_worker_fx_app,
            see _get_promotable_app) can reconfigure the *same* object in
            place instead of constructing a brand new instance and swapping
            the firexapp.engine.celery.app / microservices.celery.app
            module attribute out from under any consumer that already
            resolved it (e.g. `from firexapp.engine.celery import app`, or
            an already-imported task module whose `self.app` was bound at
            decoration time).
        """
        if fx_celery_config is None:
            fx_celery_config = FxCeleryConfig(
                fx_env=fx_env,
            )
        # force=True is only needed (and only safe to rely on) once this
        # instance has already been configured once before -- otherwise
        # keep celery's normal lazy-config behavior (config read only once
        # something actually accesses app.conf).
        self.config_from_object(
            fx_celery_config,
            force=getattr(self, '_fx_config_applied_once', False),
        )
        self._fx_config_applied_once = True
        self.conf : FxCeleryConfig
        if fx_plugins_reg is not None:
            self.fx_plugins_reg = fx_plugins_reg

        if fx_expect_tasks and not self._fx_task_execution_wired:
            celery.signals.before_task_publish.connect(self._populate_task_info, weak=False)

            self.steps['worker'].add(ReporterStep)

            # We want this step to finish after Pool at least (because a poolworker writes this file in the async case),
            # but might as well finish after Hub too
            Hub.requires = list(Hub.requires or []) + [ReporterStep]
            self._fx_task_execution_wired = True

    def _populate_task_info(self, sender: str, declare, headers, **_kwargs):
        task_info = {'name': sender}
        try:
            task_info['queue'] = declare[0].name
        except (IndexError, AttributeError):
            pass
        try:
            self.backend.client.hmset(headers['id'], task_info)
        except AttributeError:
            pass # can run on disabled backend?

    def set_attr_in_conf_and_backend(self, attr_key: str, attr_val: str):
        setattr(self.conf, attr_key, attr_val)
        self.backend.set(attr_key, str(attr_val).encode('utf-8'))

    def load_backend_conf_fields(self):
        # consumers (e.g. firex_signals.py, task.py) read this via app.conf.resources_dir
        self.conf.resources_dir = self.backend.get('resources_dir').decode()
        self.conf.load_install_config()

    @classmethod
    def _get_global_fx_app_module(cls, global_pkg_path=None):
        """
            Returns the firexapp.engine.celery module object, importing
            it if necessary.

            Uses importlib.import_module (full dotted name) rather than
            import keyword followed by attribute
            access, because this can be called while
            firexapp.engine.celery is itself still executing its own
            module-level code (e.g. set_worker_fx_app() invoked from
            that module's top level). In that case the module is
            already registered in sys.modules (just not finished
            executing), and import_module() returns that in-progress
            module object directly; an attribute chain lookup like
            `firexapp.engine.celery` would instead raise AttributeError,
            since the `celery` attribute isn't set on the `firexapp.engine`
            package until the submodule finishes importing.
        """
        return importlib.import_module(global_pkg_path or cls._GLOBAL_APP_PKG)

    @classmethod
    def _get_global_fx_app(
        cls,
    ) -> Self | None:
        """
            Returns the app currently published as the process-wide
            global app (i.e. whatever _set_global_fx_app last set), or
            None if nothing has been published yet.
        """
        return getattr(cls._get_global_fx_app_module(), 'app', None)

    @classmethod
    def _get_promotable_app(cls) -> Self | None:
        """
            Returns the already-published global app if it's already an
            instance of cls, so create_submit_fx_app can reconfigure that
            same object in place on a subsequent call within the same
            process, instead of constructing a new instance and
            reassigning the module attribute (which would silently orphan
            every consumer that already resolved the old object, e.g. via
            `from firexapp.engine.celery import app` or an already-bound
            `self.app` on an already-imported/decorated task). Returns
            None the first time an app of this type is promoted.
        """
        existing = cls._get_global_fx_app()
        return existing if isinstance(existing, cls) else None

    @classmethod
    def create_submit_fx_app(
        cls,
        uid: Uid,
        broker_mngr: RedisManager,
        plugins,
    ) -> Self:
        # assert plugins and other envs not set?
        fx_env = FxEnvVars.model_validate(
            dict(
                CURRENT_RUN_FIREX_ID=str(uid),
                firex_logs_dir=uid.logs_dir,
                redis_bin_dir=broker_mngr.redis_bin_base,
                BROKER=broker_mngr.broker_url,
                firex_plugins=plugins,
            )
        )
        fx_app = cls._get_promotable_app()
        if fx_app is not None:
            fx_app._apply_fx_config(fx_env=fx_env)
        else:
            fx_app = cls(fx_env=fx_env)

        # set OS ENV vars for this run.
        fx_app.conf.fx_env.set_fx_os_env()

        fx_app.set_attr_in_conf_and_backend('resources_dir', uid.resources_dir)
        fx_app.conf.load_install_config()
        cls._set_global_fx_app(fx_app)
        return fx_app

    @classmethod
    def create_event_receiver_fx_celery_from_os_env(cls) -> Self:
        """
            This app instance is suitable for contexts like TrackingServices
            where no tasks will be executed and no bound/unbound tasks will
            be queried for. Inspect queries should still work.

            Task definition loading is disabled for performance purposes.
        """
        fx_app = cls(
            fx_env=FxEnvVars.load_firex_env_vars_from_env(),
            autofinalize=False,
            set_as_current=False,
            fx_expect_tasks=False,
            loader=_DisabledTasksLoader,
        )
        fx_app.load_backend_conf_fields()
        # stop Celery from doing scanning magic with "empty"
        # config_source and finalized=True
        fx_app.finalized = True
        fx_app._config_source = None
        return fx_app

    @property
    def backend(self) -> RedisBackend:
        return super().backend

    @cached_property
    def AsyncResult(self):
        from firexkit.result import FxAsyncResult
        return self.subclass_with_self(
            FxAsyncResult,
            name='AsyncResult',
            reverse='AsyncResult',
        )

    def task(self, *args, **kwargs) -> FireXTask:
        return super().task(*args, **kwargs)

    def get_run_logs_dir(self) -> str:
        assert self.conf.logs_dir, 'This FireXCelery was initialised only for metadata and therefore has no run logs_dir'
        return self.conf.logs_dir

    def shutdown_broker(self):
        BrokerFactory.load_broker_manager(
            broker_url=self.conf.broker_url,
            redis_bin_base=self.conf.redis_bin_dir,
            logs_dir=self.get_run_logs_dir(),
        ).shutdown()

    def get_app_task(
        self,
        task_short_name: str,
        all_tasks: dict | None=None,
    ) -> FireXTask:
        task_short_name = task_short_name.strip()
        all_tasks = self.tasks if all_tasks is None else all_tasks

        if task_short_name in all_tasks:
            return all_tasks[task_short_name]

        for key, value in all_tasks.items():
            if key.split('.')[-1] == task_short_name:
                return value

        task_name_lower = task_short_name.lower()
        for key, value in all_tasks.items():
            if key.split('.')[-1].lower() == task_name_lower:
                return value

        raise NotRegistered(task_short_name)

    def get_app_tasks(
        self,
        tasks: list[str] | str,
        all_tasks: dict | None=None,
    ) -> list[FireXTask]:
        if isinstance(tasks, str):
            tasks = tasks.split(',')
        return [
            self.get_app_task(task, all_tasks)
            for task in tasks
        ]

    def import_microservices(
        self,
        imports: tuple[str, ...] | None=None,
    ) -> tuple[
        dict[str, FireXTask],
        dict[str, str]
    ]:
        imports = imports or self.conf.imports
        for module_name in imports:
            importlib.import_module(module_name)

        assert self.conf.fx_env, 'fx_env must be set before service tasks can be loaded.'
        plugin_path_mapping = self._load_plugins(
            self.conf.fx_env.get_plugin_files()
        )

        return self.tasks, plugin_path_mapping

    def _load_plugins(self, plugins_files: list[str]):
        original_plugins = convert_plugins_to_list(plugins_files)
        resolved_plugins = self.fx_plugins_reg.resolve_plugin_paths(original_plugins)

        # Create mapping from original plugin paths to resolved full paths
        plugin_path_mapping = {}
        # Build the mapping and validate files exist
        for original, resolved in zip(original_plugins, resolved_plugins):
            if not os.path.isfile(resolved):
                raise FileNotFoundError(resolved)
            plugin_path_mapping[original] = resolved

        self.fx_plugins_reg.load_plugin_modules(self, resolved_plugins)

        return plugin_path_mapping

    def backend_hget_task_attr(
        self,
        task_id: str,
        attr_key: str,
        timeout=15*60,
        retry_delay=1,
    ):
        raw_attr_val = firexkit.broker.handle_broker_timeout(
            self.backend.client.hget,
            args=(task_id, attr_key),
            timeout=timeout,
            retry_delay=retry_delay,
        )
        if raw_attr_val is None:
            attr_val = ''
        else:
            attr_val = raw_attr_val.decode()
        return attr_val

    def task_id_has_prerun(self, task_id: str) -> bool:
        try:
            return bool(
                self.backend_hget_task_attr(task_id, _TASK_PRE_RUN_KEY)
            )
        except AttributeError:
            logger.info('Broker does not support prerun info; probably a dummy broker. Defaulting to prerun=False')

        return False

    def task_id_has_postrun(self, task_id: str) -> bool:
        try:
            return bool(
                self.backend_hget_task_attr(task_id, _TASK_POST_RUN_KEY)
            )
        except AttributeError:
            logger.info('Broker doesn\'t support postrun info; probably a dummy broker. Defaulting to postrun=True')
        return True

    def backend_hset_task_attr(
        self,
        task_id: str,
        attr_key: str,
        attr_val,
        timeout=15*60,
        reraise_on_timeout=True,
        hsetnx=False, # set only if key unset
    ):
        redis_set_fn = getattr(
            self.backend.client,
            'hsetnx' if hsetnx else 'hset'
        )
        firexkit.broker.handle_broker_timeout(
            redis_set_fn,
            args=(task_id, attr_key, attr_val),
            timeout=timeout,
            reraise_on_timeout=reraise_on_timeout,
        )

    @classmethod
    def app_or_default(cls) -> Self:
        return app_or_default()

    @classmethod
    @contextmanager
    def metadata_fx_app(cls, plugins=None) -> Generator[Self]:
        """
            Scoped, metadata-only app (e.g. TestFrameworkRegistry loading
            real plugin modules just to read their framework
            registrations, or 'info'/'list' style queries).

            Deliberately always constructs a fresh, isolated instance
            (never reuses/reconfigures an existing global app in place,
            unlike create_submit_fx_app/set_worker_fx_app) so that it
            can't clobber the task registry of whatever app object test
            code / other already-imported task modules are relying on.

            Since this metadata app has no broker (and no run env at all),
            leaving it published as the global app would break anything
            that subsequently resolves the broker from the app -- hence
            any pre-existing global app is restored on exit.
        """
        pre_existing_fx_app = cls._get_global_fx_app()
        # FIXME: shouldn't be necessary to set the global app at all,
        # but our internal dependencies are backwards in lots of places,
        # so we'll just set it (and undo it) until we fix everything.
        fx_app = cls(
            fx_expect_tasks=False,
            fx_env=FxEnvVars.create_no_task_exec_fx_env(plugins),
        )
        cls._set_global_fx_app(fx_app)
        try:
            yield fx_app
        finally:
            if (
                pre_existing_fx_app is not None
                and pre_existing_fx_app is not fx_app
            ):
                cls._set_global_fx_app(pre_existing_fx_app)

    @classmethod
    def _set_global_fx_app(cls, fx_app: Self, global_pkg_path=None):
        fx_app.set_current()
        fx_app.set_default()
        cls._get_global_fx_app_module(
            global_pkg_path=global_pkg_path,
        ).app = fx_app


class _DisabledTasksLoader(BaseLoader):
    """
        Celery does a lot of crazy stuff automatically, try
        to disable module scanning.
    """

    def autodiscover_tasks(self, *args, **kwargs):
        pass

    def _smart_import(self, *args, **kwargs):
        return {}


@celery.signals.task_postrun.connect
def _mark_task_postrun(task: FireXTask, task_id: str, **_kwargs):
    task.app.backend_hset_task_attr(task_id, _TASK_POST_RUN_KEY, 'True')
    if task.app.backend_hget_task_attr(task_id, '_fx_forget'):
        task.AsyncResult(task_id).fx_forget()


@celery.signals.task_prerun.connect
def _update_task_name(sender: FireXTask, task_id: str, *_args, **_kwargs):
    sender.app.backend_hset_task_attr(task_id, _TASK_PRE_RUN_KEY, 'True')
    sender.set_backend_task_start_time(task_id)
    # Although the name was populated in _populate_task_info before_task_publish, the name
    # can be inaccurate if it was a plugin. We can only over-write it with the accurate name
    # at task_prerun.
    sender.app.backend_hset_task_attr(
        task_id, 'name', sender.name,
        timeout=5*60,
        reraise_on_timeout=False,
    )

@celery.signals.worker_ready.connect()
def _celery_worker_ready(sender: Consumer, **_kwargs):
    queue_names = [queue.name for queue in sender.task_consumer.queues]
    if queue_names:
        sender.app.backend.client.sadd(
            firexkit.broker.FX_QUEUES_KEY,
            *queue_names)


@celery.signals.task_received.connect
def on_task_received(sender: FireXTask, request=None, **kwargs):
    if request and request.parent_id:
        sender.app.backend_hset_task_attr(request.id, '_fx_parent_id', request.parent_id)


@celery.signals.task_postrun.connect()
def statsd_task_postrun(
    sender: FireXTask,
    task: FireXTask,
    task_id: str,
    *_args,
    **donotcare,
):
    # Celery can send task-revoked event before task is completed, allowing other states (e.g. task-unblocked) to
    # be emitted after task-revoked. Sending another indicator of revoked here allows the terminal state to be
    # correctly captured by listeners, since task_postrun occurs when the task is _really_ complete.
    if task.AsyncResult(task_id).fx_is_revoked():
        try:
            RevokeDetails.write_task_revoke_complete(
                sender.app.get_run_logs_dir(),
                task_id,
            )
        except Exception as e:
            logger.warning(f'Failed to write revoke complete for task {task_id}: {e}')
        task.send_event(RunStates.REVOKE_COMPLETED.to_celery_event_type())

    _send_task_completed_event(task)


@celery.signals.task_revoked.connect()
def statsd_task_revoked(sender: FireXTask, request=None, *_args, **_kwargs):
    # sender.request doesn't necessarily refer to this task: task_revoked is sent
    # from the worker's controlling process (not the process that ran the task),
    # so push the actual revoked task's context to get its correct id/duration.
    if request:
        sender.request_stack.push(request)
    try:
        _send_task_completed_event(sender)
    finally:
        if request:
            sender.request_stack.pop()


def _send_task_completed_event(task: FireXTask | None):
    if task:
        if ( actual_runtime := task.duration() ) is not None:
            task.send_event(
                'task-completed',
                actual_runtime=convert_to_serializable(
                    max(actual_runtime, 0)
                )
            )

