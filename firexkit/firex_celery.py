import importlib
import logging
import os
from collections.abc import Generator
from contextlib import contextmanager
from time import monotonic
from typing import ClassVar

import celery.signals
import celery.worker.state
from billiard.pool import ApplyResult
from billiard.pool import Pool as BilliardPool
from celery.app import app_or_default
from celery.app.base import Celery
from celery.backends.redis import RedisBackend
from celery.concurrency.asynpool import AsynPool
from celery.concurrency.prefork import TaskPool
from celery.exceptions import NotRegistered
from celery.loaders.base import BaseLoader
from celery.utils.log import get_task_logger
from celery.worker.components import Hub
from celery.worker.consumer import Consumer
from celery.worker.control import control_command, nok, ok
from kombu.asynchronous.timer import Entry
from kombu.utils.objects import cached_property
from typing_extensions import Self

import firexkit.broker
from firexapp.broker_manager.broker_factory import BrokerFactory, RedisManager
from firexapp.celery_manager import CeleryManager
from firexapp.engine.default_celery_config import FxCeleryConfig, FxEnvVars
from firexapp.events.model import (
    TASK_REVOKE_REASON_KEY,
    RevokeDetails,
    RunStates,
)
from firexapp.plugins import (
    FxPluginRegistry,
    convert_plugins_to_list,
)
from firexapp.reporters.json_reporter import FireXRunData, ReporterStep
from firexapp.submit.uid import Uid
from firexkit.run_time import RunTimeReserve, get_run_soft_time_limit
from firexkit.task import FireXTask, convert_to_serializable

logger = get_task_logger(__name__)

_TASK_PRE_RUN_KEY = 'TASK_PRE_RUN'
_TASK_POST_RUN_KEY = 'TASK_POST_RUN'

# Worker remote control command backing FireXCelery.set_task_soft_time_limit.
FX_SET_TASK_SOFT_TIME_LIMIT_CMD = 'fx_set_task_soft_time_limit'

# Worker remote control command backing FireXCelery.increase_run_soft_time_limit.
FX_INCREASE_RUN_SOFT_TIME_LIMIT_CMD = 'fx_increase_run_soft_time_limit'

# Result backend key holding the run's total time budget, in seconds.
FX_RUN_SOFT_TIME_LIMIT_KEY = 'run_soft_time_limit'

# Message header carrying the reserve a run-relative task was published with, so a later
# budget increase can recompute the limit of a task that is already running.
FX_TIME_RESERVE_HEADER = 'fx_time_reserve'

# Result backend key flagging that the run's root task has been revoked, i.e. that the
# whole run is being cancelled.
_RUN_REVOKE_STARTED_KEY = 'ROOT_REVOKED'


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
        plugin_load_log_level=logging.INFO,
        **kwargs,
    ):
        self.plugin_load_log_level = plugin_load_log_level
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
            if fx_env.firex_id != ( app_fid := fx_app.conf.fx_env.firex_id ):
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
        self._resolve_run_relative_time_limit(sender, headers)
        task_info = {'name': sender}
        try:
            task_info['queue'] = declare[0].name
        except (IndexError, AttributeError):
            pass
        try:
            self.backend.client.hmset(headers['id'], task_info)
        except AttributeError:
            pass # can run on disabled backend?

    def _resolve_run_relative_time_limit(self, task_name: str, headers):
        """
            Turns a run-relative time limit into the number of seconds this task gets.

            Resolved here, at publish, rather than when the signature was built, so the
            limit always reflects the budget as it stands now -- including a budget that
            was raised after the task module was imported.

            celery sends before_task_publish with the very headers it is about to publish
            (celery/app/amqp.py), and headers['timelimit'] is a mutable
            [time_limit, soft_time_limit], so mutating it here changes the published task.
        """
        time_limit = headers.get('timelimit')
        if not time_limit:
            return

        reserve = time_limit[1]
        if not isinstance(reserve, RunTimeReserve):
            if reserve is not None:
                return # an explicit number; leave it alone.
            # Fall back to what the task type declares, if anything.
            task = self.tasks.get(task_name)
            declared = getattr(task, 'run_time_limit_reserve', None)
            if declared is None:
                return
            reserve = RunTimeReserve(declared)

        time_limit[1] = reserve.resolve(self)
        # Carries the reserve rather than only the resolved number, so that a later
        # budget increase can recompute this task's limit while it is running.
        headers[FX_TIME_RESERVE_HEADER] = reserve.reserve

    def set_attr_in_conf_and_backend(self, attr_key: str, attr_val: str):
        setattr(self.conf, attr_key, attr_val)
        self.backend.set(attr_key, str(attr_val).encode('utf-8'))

    def load_backend_conf_fields(self):
        # consumers (e.g. firex_signals.py, task.py) read this via app.conf.resources_dir
        resources_dir = self.backend.get('resources_dir')
        self.conf.resources_dir = resources_dir.decode()
        # Workers are started throughout a run, so this can already be an increased
        # budget; it's what FireXTaskPool seeds its default task soft time limit from.
        self._cache_run_soft_time_limit(self._read_run_soft_time_limit())
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
        run_soft_time_limit: int | None=None,
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
            fx_app = cls(
                fx_env=fx_env,
                plugin_load_log_level=logging.PRINT,
            )

        # set OS ENV vars for this run.
        fx_app.conf.fx_env.set_fx_os_env()

        # The run's total time budget. Seeded once here so every worker and every pool
        # child reads the same value, and so a task can raise it later.
        fx_app.conf.run_soft_time_limit = run_soft_time_limit
        fx_app.set_attr_in_conf_and_backend(
            FX_RUN_SOFT_TIME_LIMIT_KEY,
            fx_app._default_run_soft_time_limit(),
        )
        fx_app.set_attr_in_conf_and_backend('resources_dir', uid.resources_dir)
        fx_app.backend.set('logs_dir', str(fx_app.conf.fx_env.logs_dir).encode('utf-8'))
        fx_app.backend.set('uid', str(fx_app.conf.fx_env.firex_id).encode('utf-8'))
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

    #
    # Revoking (i.e. cancelling) tasks, and therefore runs: revoking the root task
    # revokes the whole run. Every revoke is recorded, so that the reason it happened
    # can be shown to whoever looks at the task or the run afterwards.
    #

    def revoke_task(
        self,
        task_uuid: str,
        reason: str | None,
        revoking_user: str | None=None,
        is_root_task: bool=False,
    ) -> RevokeDetails:
        # revoke can be fast, so do data tracking setup before control.revoke()
        if is_root_task:
            self._set_run_revoke_started()

        revoke_details = RevokeDetails(
            self.get_run_logs_dir(),
            reason=reason,
            task_uuid=task_uuid,
            root_revoke=is_root_task,
            revoking_user=revoking_user,
        )
        revoke_details.write()

        self.control.revoke(task_uuid, terminate=True)
        logger.info(f'Submitted revoke to celery for: {task_uuid}')
        return revoke_details

    def is_run_revoke_started(self) -> bool:
        return bool(self._get_run_revoke_started()) or self.is_run_revoke_complete()

    def is_run_revoke_complete(self) -> bool:
        return FireXRunData.load_from_logs_dir(
            self.get_run_logs_dir(),
        ).revoked

    def get_run_revoke_details(self) -> RevokeDetails | None:
        if not self._get_run_revoke_started():
            return None
        return RevokeDetails.load_latest_run_revoke_details(
            self.get_run_logs_dir(),
        )

    def get_task_revoke_details(self, task_uuid: str) -> RevokeDetails | None:
        return RevokeDetails.load_latest_revoke_details(
            self.get_run_logs_dir(), task_uuid=task_uuid,
        )

    def complete_task_revoke(self, task_uuid: str) -> RevokeDetails | None:
        """Marks task_uuid's revoke complete; returns why it was revoked, if known."""
        return RevokeDetails.complete_task_revoke(
            self.get_run_logs_dir(), task_uuid,
        )

    #
    # The run revoke flag lives in the backend DB (i.e. Redis), since the run is
    # revoked from a different process than the ones that need to know about it.
    #
    def _set_run_revoke_started(self):
        self.backend.set(_RUN_REVOKE_STARTED_KEY, 'True')

    def _get_run_revoke_started(self):
        return self.backend.get(_RUN_REVOKE_STARTED_KEY)

    def set_task_soft_time_limit(
        self,
        task_id: str,
        soft_time_limit: float | None,
        destination: list[str] | None=None,
        timeout: float=5.0,
        increase_only: bool=False,
    ) -> float | None:
        """
            Changes the soft_time_limit of a task that has already started executing.

            soft_time_limit is absolute and measured from when the task started,
            exactly like the soft_time_limit the task was submitted with, so
            supplying a value the task has already exceeded causes it to be
            signalled immediately. Supply None to remove the soft time limit.

            The task's hard time_limit, if any, is never changed; the applied soft
            time limit is clamped to it.

            With increase_only, a limit that wouldn't give the task more time than it
            already has is ignored; callers that only ever want to extend a task
            should use it, since the task's current limit may be the worker's default
            rather than anything the caller can see.

            Returns the soft time limit the owning worker applied, or None if no
            worker reported running task_id (e.g. it already completed).

            timeout is how long to wait for replies. Naming a destination makes the
            call return as soon as those workers answer; without one there is no
            reply count to stop at, so the full timeout is always spent. Callers on
            the critical path of the task being changed should name its worker.
        """
        replies = self.control.broadcast(
            FX_SET_TASK_SOFT_TIME_LIMIT_CMD,
            arguments={
                'task_id': task_id,
                'soft_time_limit': soft_time_limit,
                'increase_only': increase_only,
            },
            destination=destination,
            reply=True,
            # kombu's reply collector drains until `limit` messages arrive, then falls
            # back to waiting out the timeout. Only a known destination gives a count.
            limit=len(destination) if destination else None,
            timeout=timeout,
        ) or []

        for reply in replies:
            for worker_name, response in reply.items():
                if not isinstance(response, dict):
                    continue
                if 'ok' in response:
                    logger.debug(
                        f'Worker {worker_name} set soft_time_limit of {task_id}'
                        f' to {response["ok"]}s'
                    )
                    return response['ok']
                logger.debug(
                    f'Worker {worker_name} did not set soft_time_limit of {task_id}:'
                    f' {response.get("error")}'
                )
        return None

    # Monotonic max. Redis has no atomic max for string values, and tasks on different
    # workers can raise the budget concurrently. Always returns the stored value.
    _FX_INCREASE_RUN_BUDGET_LUA = """
        local requested = tonumber(ARGV[1])
        local current = tonumber(redis.call('GET', KEYS[1]))
        if current == nil or requested > current then
            redis.call('SET', KEYS[1], ARGV[1])
        end
        return redis.call('GET', KEYS[1])
    """

    # Seconds a locally cached budget is trusted for. Short, because any task on any
    # worker can raise it, and the cost of a stale read is a task getting less time
    # than the run has actually been granted.
    _FX_RUN_BUDGET_CACHE_SECS = 15

    # (monotonic_read_time, budget); populated lazily, per process.
    _fx_run_budget_cache: tuple[float, float] | None = None

    def get_run_soft_time_limit(self) -> float:
        """
            The run's total time budget in seconds.

            Read from the result backend rather than app.conf, because pool child
            processes never receive control commands and a run can have many workers.
            Briefly cached, so RunTimeReserve.resolve() isn't a broker round trip per call.
        """
        cached = self._fx_run_budget_cache
        now = monotonic()
        if cached is not None and now - cached[0] < self._FX_RUN_BUDGET_CACHE_SECS:
            return cached[1]

        budget = self._read_run_soft_time_limit()
        self._fx_run_budget_cache = (now, budget)
        return budget

    def _default_run_soft_time_limit(self) -> float:
        # task_soft_time_limit is what --soft_time_limit has always been plumbed into,
        # so falling back to it keeps bundles correct before they adopt the new name.
        return self.conf.run_soft_time_limit or self.conf.task_soft_time_limit

    def _read_run_soft_time_limit(self) -> float:
        try:
            raw_budget = firexkit.broker.handle_broker_timeout(
                self.backend.get,
                args=(FX_RUN_SOFT_TIME_LIMIT_KEY,),
                timeout=60,
            )
        except AttributeError:
            return self._default_run_soft_time_limit() # probably a dummy broker.
        if raw_budget is None:
            return self._default_run_soft_time_limit()
        return float(raw_budget)

    def _cache_run_soft_time_limit(self, run_soft_time_limit: float):
        self.conf.run_soft_time_limit = run_soft_time_limit
        self._fx_run_budget_cache = (monotonic(), run_soft_time_limit)

    def increase_run_soft_time_limit(self, run_soft_time_limit: float) -> float:
        """
            Raises the run's total time budget, and extends the tasks that depend on it.

            The increase is monotonic: a request for less time than the run already has
            is a no-op, which makes this safe to call unconditionally and safe to call
            from a task that retries. There is no ceiling.

            Every worker then extends every running task that was following the run
            budget: one published with a reserve, one whose task type declares
            run_time_limit_reserve, and one that named no limit of its own and so is
            running on the worker default. Only a task published with an explicit number
            keeps its old limit, since that number was chosen for the task rather than
            derived from the run. The workers' default task soft time limit is raised to
            the new budget too, which is what covers tasks dispatched after this point.

            Returns the budget in effect after the call, which may be larger than
            requested if another task asked for more. That number comes from the
            broker, not from the workers, so it is already authoritative when the
            broadcast goes out.
        """
        if run_soft_time_limit <= self.get_run_soft_time_limit():
            return self.get_run_soft_time_limit()

        effective = float(
            firexkit.broker.handle_broker_timeout(
                self.backend.client.eval,
                args=(
                    self._FX_INCREASE_RUN_BUDGET_LUA,
                    1,
                    FX_RUN_SOFT_TIME_LIMIT_KEY,
                    run_soft_time_limit,
                ),
                timeout=60,
            )
        )
        self._cache_run_soft_time_limit(effective)
        self._record_run_soft_time_limit_in_run_json(effective)

        # No destination: a run can have many workers, and the tasks being extended are
        # spread across them. That also means there is no reply count to wait for, and
        # collecting replies anyway would block for the full timeout on every raise --
        # time charged against the very budget the caller just asked to extend. The
        # message is published either way; waiting only confirms, it does not deliver.
        self.control.broadcast(
            FX_INCREASE_RUN_SOFT_TIME_LIMIT_CMD,
            arguments={'run_soft_time_limit': effective},
        )
        return effective

    def _record_run_soft_time_limit_in_run_json(self, run_soft_time_limit: float):
        """
            Mirrors the raised budget into run.json.

            The budget itself lives in the broker, which is where everything inside the
            run reads it from. run.json is for everything outside: kill_runs decides
            whether a run has outlived its time limit long after that run's broker is
            gone, and would otherwise reap the very runs this feature exists to allow.

            Best effort. Failing to write the file must not fail the task that asked for
            more time, since the increase itself has already taken effect.
        """
        try:
            FireXRunData.persist_run_soft_time_limit(
                self.get_run_logs_dir(),
                run_soft_time_limit,
            )
        except Exception as e:
            logger.warning(
                f'Failed recording run_soft_time_limit {run_soft_time_limit} in run.json;'
                f' the increase is in effect for this run, but processes outside it'
                f' (e.g. run reaping) will still see the submitted limit: {e}'
            )

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
        log_level: int | None=None,
    ) -> tuple[
        dict[str, FireXTask],
        dict[str, str]
    ]:
        if self.finalized:
            # Celery only finalizes once, so anything imported from here on will
            # register against whichever app it was decorated with and never be
            # replayed on to this one. Fail here, where the premature finalize is
            # still on the stack, instead of at task lookup much later.
            raise RuntimeError(
                f'{self} was already finalized before import_microservices; '
                'tasks imported from here on will not be registered. Something '
                'accessed app.tasks (or called finalize) too early.'
            )

        imports = imports or self.conf.imports
        for module_name in imports:
            importlib.import_module(module_name)

        assert self.conf.fx_env, 'fx_env must be set before service tasks can be loaded.'
        if log_level is None:
            log_level = self.plugin_load_log_level
        plugin_path_mapping = self._load_plugins(
            self.conf.fx_env.get_plugin_files(),
            log_level=log_level,
        )

        return self.tasks, plugin_path_mapping

    def _load_plugins(
        self,
        plugins_files: list[str],
        log_level,
    ):
        original_plugins = convert_plugins_to_list(plugins_files)
        resolved_plugins = self.fx_plugins_reg.resolve_plugin_paths(original_plugins)

        # Create mapping from original plugin paths to resolved full paths
        plugin_path_mapping = {}
        # Build the mapping and validate files exist
        for original, resolved in zip(original_plugins, resolved_plugins):
            if not os.path.isfile(resolved):
                raise FileNotFoundError(resolved)
            plugin_path_mapping[original] = resolved

        self.fx_plugins_reg.load_plugin_modules(
            self,
            resolved_plugins,
            log_level,
        )

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
            plugin_load_log_level=logging.PRINT,
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
        # Resolve (and therefore import, if this is the first reference) the
        # module that holds the global "app" *before* asserting current/default.
        # That module's body constructs a placeholder app of its own, and a
        # Celery() construction claims set_current() -- so importing it after
        # set_current() would immediately overwrite what we just published.
        # The worker re-asserts current/default via
        # trace.setup_worker_optimizations(), but the submit process does not.
        global_app_module = cls._get_global_fx_app_module(
            global_pkg_path=global_pkg_path,
        )
        fx_app.set_current()
        fx_app.set_default()
        global_app_module.app = fx_app


class _DisabledTasksLoader(BaseLoader):
    """
        Celery does a lot of crazy stuff automatically, try
        to disable module scanning.
    """

    def autodiscover_tasks(self, *args, **kwargs):
        pass

    def _smart_import(self, *args, **kwargs):
        return {}


#
# Celery's built-in 'time_limit' control command only changes Task.soft_time_limit
# for a task *type*, which affects subsequently dispatched tasks. Moving the
# deadline of a task a pool child has already started means cancelling and
# re-arming the hub timer AsynPool armed for that job, which stock celery doesn't
# expose -- hence FireXTaskPool.
#

_FX_SOFT = 'soft'
_FX_HARD = 'hard'


# A job whose run time is already gone still needs a positive soft time limit: billiard
# reads a soft_timeout of zero as "no limit at all", the opposite of what being out of
# time should mean. Small enough to signal on the next hub tick.
_FX_MIN_JOB_SOFT_TIME_LIMIT = 0.001


def _run_relative_job_limit(
    result: ApplyResult,
    reserve: RunTimeReserve,
    app,
) -> float:
    """
        A RunTimeReserve as an absolute soft time limit for a job, which is what a job's
        soft_time_limit means: seconds measured from when the job started.

        Elapsed time comes from the monotonic clock and remaining run time from the wall
        clock, but only their durations are combined, so the two are never compared.
    """
    time_accepted = result._time_accepted
    elapsed = (monotonic() - time_accepted) if time_accepted else 0
    # remaining(), not resolve(): this job is already running, so pointing it at the
    # run's real deadline is the entire purpose. resolve()'s floor is there to keep a
    # not-yet-started task's limit usable, and applying it to a running job hands it up
    # to DEFAULT_MINIMUM_RUN_TIME_REMAINING seconds past the deadline -- the very time
    # the task that asked for the increase just established the run does not have.
    return max(elapsed + reserve.remaining(app), _FX_MIN_JOB_SOFT_TIME_LIMIT)


def _is_soft_time_limit_increase(
    result: ApplyResult,
    soft_time_limit: float | None,
) -> bool:
    """Whether soft_time_limit would give result more time than it already has."""
    current = result._soft_timeout
    if current is None:
        return False # already unlimited.
    if soft_time_limit is None:
        return True # becoming unlimited.
    return soft_time_limit > current


def _clamped_soft_time_limit(
    result: ApplyResult,
    soft_time_limit: float | None,
) -> float | None:
    """
        A running job's hard time limit is never moved, so a soft time limit at or
        beyond it can't be honoured; clamp to the hard time limit instead.
    """
    hard_time_limit = result._timeout
    if (
        soft_time_limit is not None
        and hard_time_limit
        and soft_time_limit >= hard_time_limit
    ):
        logger.warning(
            f'Requested soft_time_limit of {soft_time_limit}s for task'
            f' {result.correlation_id} is not below its hard time_limit of'
            f' {hard_time_limit}s; clamping to the hard time_limit.'
        )
        return hard_time_limit
    return soft_time_limit


class FireXAsynPool(AsynPool):
    """
        AsynPool that arms the soft and hard time limit timers of a job
        independently, at absolute deadlines relative to the job's accept time, so
        the soft deadline of an already-running job can be moved without disturbing
        its hard deadline.

        Upstream AsynPool arms only the soft timer up-front and creates the hard
        timer from within the soft timer's callback
        (hub.call_later(hard - soft, ...)), re-using a single tref slot per job.
        That yields the same two deadlines we do (T+soft and T+hard), but makes the
        hard deadline unrecoverable once the soft tref is replaced.
    """

    def __init__(self, *args, **kwargs):
        # job id -> {'soft': timer Entry, 'hard': timer Entry}.
        # Deliberately populated here and never reset in _create_timelimit_handlers,
        # which re-runs every time the consumer re-registers with the event loop
        # (e.g. after a broker reconnect) while jobs are still in flight.
        self._fx_trefs: dict[int, dict[str, Entry]] = {}
        self._fx_hub = None
        super().__init__(*args, **kwargs)

    def _create_timelimit_handlers(self, hub):
        # Replaces (does not extend) the upstream implementation: the closures it
        # installs are the only users of _tref_for_id, so taking over
        # on_timeout_set/on_timeout_cancel/_discard_tref replaces the whole upstream
        # timer scheme.
        self._fx_hub = hub
        self.on_timeout_set = self._fx_on_timeout_set
        self.on_timeout_cancel = self._fx_on_timeout_cancel
        self._discard_tref = self._fx_discard_trefs

    def _fx_on_timeout_set(self, result, soft, hard):
        # Called from ApplyResult._ack, i.e. once a child has accepted the job.
        self._fx_arm(result, _FX_SOFT, soft)
        self._fx_arm(result, _FX_HARD, hard)

    def _fx_on_timeout_cancel(self, result):
        self._fx_discard_trefs(result._job)

    def _fx_arm(self, result, kind, timeout):
        job = result._job
        self._fx_cancel_tref(job, kind)
        if not timeout or result._time_accepted is None or self._fx_hub is None:
            return
        expired = self._fx_soft_expired if kind == _FX_SOFT else self._fx_hard_expired
        # _time_accepted is monotonic() taken in the child, and CLOCK_MONOTONIC is
        # system-wide, so it's comparable here (billiard's TimeoutHandler relies on
        # the same thing).
        delay = max((result._time_accepted + timeout) - monotonic(), 0)
        self._fx_trefs.setdefault(job, {})[kind] = self._fx_hub.call_later(
            delay, expired, job,
        )

    def _fx_cancel_tref(self, job, kind):
        job_trefs = self._fx_trefs.get(job)
        if not job_trefs:
            return
        tref = job_trefs.pop(kind, None)
        if tref is not None:
            tref.cancel()
        if not job_trefs:
            self._fx_trefs.pop(job, None)

    def _fx_discard_trefs(self, job):
        for kind in (_FX_SOFT, _FX_HARD):
            self._fx_cancel_tref(job, kind)
        self._fx_trefs.pop(job, None)

    def _fx_soft_expired(self, job):
        self._fx_cancel_tref(job, _FX_SOFT)
        result = self._cache.get(job)
        if result is None:
            self._fx_discard_trefs(job)
        else:
            # Sends SIG_SOFT_TIMEOUT to the child, raising SoftTimeLimitExceeded there.
            self.on_soft_timeout(result)

    def _fx_hard_expired(self, job):
        self._fx_cancel_tref(job, _FX_HARD)
        result = self._cache.get(job)
        if result is None:
            self._fx_discard_trefs(job)
        else:
            self.on_hard_timeout(result)

    def set_job_soft_time_limit(
        self,
        result: ApplyResult,
        soft_time_limit: float | None,
        increase_only: bool=False,
    ) -> float | None:
        """
            Changes the soft time limit of a job, leaving its hard time limit alone.
            soft_time_limit is absolute and measured from when the job was accepted,
            so it means exactly what the job's original soft_time_limit meant; a job
            that has already run longer is signalled immediately.

            With increase_only, a limit that wouldn't give the job more time than it
            already has is left alone; used by run time budget increases, which must
            never shorten anything.

            Returns the limit actually applied, clamped to the job's hard time limit
            when it has one.

            Must be called from the worker MainProcess event loop thread, since it
            mutates the hub's timer queue.
        """
        if increase_only and not _is_soft_time_limit_increase(result, soft_time_limit):
            return result._soft_timeout
        soft_time_limit = _clamped_soft_time_limit(result, soft_time_limit)
        # Keeps ApplyResult.handle_timeout reporting the new value, and is all that's
        # needed when the job hasn't been accepted yet, since _ack then arms from
        # this attribute.
        result._soft_timeout = soft_time_limit
        self._fx_arm(result, _FX_SOFT, soft_time_limit)
        return soft_time_limit


class FireXBlockingPool(BilliardPool):
    """
        Threaded (no event loop) counterpart of FireXAsynPool. billiard's
        TimeoutHandler re-reads job._soft_timeout against job._time_accepted about
        once a second, so assigning the attribute is enough to move the deadline.

        Note billiard won't re-signal a job whose soft time limit has already fired,
        so in this mode a limit can only be changed before it expires.
    """

    def set_job_soft_time_limit(
        self,
        result: ApplyResult,
        soft_time_limit: float | None,
        increase_only: bool=False,
    ) -> float | None:
        if increase_only and not _is_soft_time_limit_increase(result, soft_time_limit):
            return result._soft_timeout
        soft_time_limit = _clamped_soft_time_limit(result, soft_time_limit)
        result._soft_timeout = soft_time_limit
        return soft_time_limit


class FireXTaskPool(TaskPool):
    """
        Prefork pool that can change the soft_time_limit of tasks that have already
        begun executing. See FireXCelery.set_task_soft_time_limit for the
        caller-facing API.
    """

    Pool = FireXAsynPool
    BlockingPool = FireXBlockingPool

    def on_start(self):
        self._fx_seed_soft_timeout_from_run_budget()
        super().on_start()

    def _fx_seed_soft_timeout_from_run_budget(self):
        """
            Raises this worker's default task soft time limit to the run's time budget.

            Workers are started throughout a run -- firex's WorkerSandboxBase starts one
            per ADS sandbox -- including after the budget has been increased. The budget
            is read from the broker, so a worker started late agrees with the workers
            already running. A --soft-time-limit command line value could only ever carry
            the budget as it stood when the worker was spawned.

            Raise-only, so a deliberately larger worker default is never shortened.
        """
        try:
            budget = get_run_soft_time_limit(self.app)
        except Exception:
            logger.warning(
                'Could not read the run time budget; leaving this worker'
                ' default soft time limit alone.',
                exc_info=True,
            )
            return

        current = self.options.get('soft_timeout')
        # current of None means no default limit at all, which is already more
        # permissive than any budget.
        if current is not None and budget and budget > current:
            logger.info(
                f'Raising this worker default task soft time limit from {current}s to'
                f' the run time budget of {budget}s.'
            )
            self.options['soft_timeout'] = budget

    def set_task_soft_time_limit(
        self,
        task_id: str,
        soft_time_limit: float | RunTimeReserve | None,
        increase_only: bool=False,
    ) -> float | None:
        """
            Returns the soft time limit applied to task_id, or None if this worker
            isn't running it. A RunTimeReserve is resolved against the run's current
            time budget and the time the task has already been running.
        """
        result = self._find_job_result(task_id)
        if result is None:
            return None
        if isinstance(soft_time_limit, RunTimeReserve):
            soft_time_limit = _run_relative_job_limit(result, soft_time_limit, self.app)
        return self._pool.set_job_soft_time_limit(
            result,
            soft_time_limit,
            increase_only=increase_only,
        )

    def set_default_soft_time_limit(self, soft_time_limit: float) -> float | None:
        """
            Raises the default soft time limit given to subsequently dispatched tasks
            that don't specify one of their own.

            billiard.pool.Pool.apply_async re-reads self.soft_timeout on every dispatch,
            so this takes effect without restarting the worker. Raise-only.

            Returns the default in effect after the call.
        """
        current = self._pool.soft_timeout
        if current is None or not soft_time_limit or soft_time_limit <= current:
            return current
        self._pool.soft_timeout = soft_time_limit
        self.options['soft_timeout'] = soft_time_limit
        return soft_time_limit

    def _find_job_result(self, task_id: str) -> ApplyResult | None:
        # Request.execute_using_pool sets correlation_id to the task id. The cache
        # holds at most concurrency x prefetch_multiplier entries.
        return next(
            (
                result
                for result in list(self._pool._cache.values())
                if getattr(result, 'correlation_id', None) == task_id
            ),
            None,
        )


@control_command(
    name=FX_SET_TASK_SOFT_TIME_LIMIT_CMD,
    args=[('task_id', str), ('soft_time_limit', float)],
    signature='<task_id> <soft_time_limit>',
)
def _fx_set_task_soft_time_limit(
    state, task_id=None, soft_time_limit=None, increase_only=False, **_kwargs,
):
    """Change the soft_time_limit of an already running task, by task id."""
    pool = state.consumer.pool
    set_soft_time_limit = getattr(pool, 'set_task_soft_time_limit', None)
    if set_soft_time_limit is None:
        return nok(
            f'{type(pool).__name__} does not support changing the soft time limit'
            f' of running tasks.'
        )
    applied = set_soft_time_limit(task_id, soft_time_limit, increase_only=increase_only)
    if applied is None:
        return nok(f'Task {task_id} is not running on this worker.')
    logger.info(f'Set soft_time_limit of running task {task_id} to {applied}s')
    return ok(applied)


def _request_run_time_reserve(request) -> float | None:
    """
        The reserve this request wants left at the end of the run, or None if its time
        limit has nothing to do with the run budget.
    """
    reserve = request.request_dict.get(FX_TIME_RESERVE_HEADER)
    if reserve is not None:
        return reserve # published with an explicit reserve.

    declared = getattr(request.task, 'run_time_limit_reserve', None)
    if declared is not None:
        return declared

    if request.time_limits[1] is None:
        # Nothing was published for this task, so it is running on the worker's default
        # soft time limit, which is the run budget. It was following the run before this
        # raise and should keep following it afterwards -- otherwise splitting the run
        # budget out of --soft_time_limit would strand every task that declared nothing.
        return 0

    # An explicit number, deliberately chosen for this task rather than derived from the
    # run. Raising the run budget is not a reason to overrule it.
    return None


@control_command(
    name=FX_INCREASE_RUN_SOFT_TIME_LIMIT_CMD,
    args=[('run_soft_time_limit', float)],
    signature='<run_soft_time_limit>',
)
def _fx_increase_run_soft_time_limit(
    state,
    run_soft_time_limit=None,
    **_kwargs,
):
    """Apply an increased run time budget to this worker's tasks."""
    app = state.consumer.app
    app._cache_run_soft_time_limit(run_soft_time_limit)

    pool = state.consumer.pool
    set_task_soft_time_limit = getattr(pool, 'set_task_soft_time_limit', None)
    if set_task_soft_time_limit is None:
        return nok(
            f'{type(pool).__name__} does not support changing the soft time limit'
            f' of running tasks.'
        )

    # Tasks that declared nothing follow the run through the pool default, which
    # billiard re-reads on every dispatch.
    pool.set_default_soft_time_limit(run_soft_time_limit)

    extended = {}
    for request in list(celery.worker.state.active_requests):
        reserve = _request_run_time_reserve(request)
        if reserve is None:
            continue
        applied = set_task_soft_time_limit(
            request.id,
            RunTimeReserve(reserve),
            increase_only=True,
        )
        if applied is not None:
            extended[request.id] = applied

    for request in list(celery.worker.state.reserved_requests):
        if request.id in extended:
            continue # already running; handled above.
        reserve = _request_run_time_reserve(request)
        if reserve is None:
            continue
        # Prefetched but not yet dispatched, so there is no pool job to re-arm and its
        # timelimit header was built from the old budget. It hasn't started, so the
        # reserve resolves straight to its limit.
        hard_time_limit = (request.time_limits or (None, None))[0]
        request.time_limits = [hard_time_limit, RunTimeReserve(reserve).resolve(app)]
        extended[request.id] = request.time_limits[1]

    logger.info(
        f'Run time budget increased to {run_soft_time_limit}s;'
        f' extended {len(extended)} task(s) on this worker.'
    )
    return ok(extended)


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
            revoke_details = sender.app.complete_task_revoke(task_id)
        except Exception as e:
            revoke_details = None
            logger.warning(f'Failed to write revoke complete for task {task_id}: {e}')

        revoke_reason = revoke_details.reason if revoke_details else None
        task.send_event(
            RunStates.REVOKE_COMPLETED.to_celery_event_type(),
            # Why this task was revoked, so consumers of the event stream can show the
            # reason against the task itself. Left out entirely when nothing was
            # recorded, rather than sent as null.
            **({TASK_REVOKE_REASON_KEY: revoke_reason} if revoke_reason else {}),
        )

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

