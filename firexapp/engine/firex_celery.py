import typing
import os
import sys
from types import ModuleType
import inspect
from urllib.parse import urlsplit
import traceback


from celery import platforms
# Prevent main celery proc from killing pre-forked procs,
# otherwise killing celery main proc causes sync main firex proc
# to hang since broker will remain up.
platforms.set_pdeathsig = lambda n: None
from kombu.utils.objects import cached_property
from celery.app.base import Celery
from celery.utils.log import get_task_logger
import celery.exceptions
from celery.worker.components import Hub
from celery import bootsteps
import celery.signals

from firexapp.plugins import PluginLoadError, get_active_plugins

from firexapp.submit.uid import FIREX_ID_REGEX
from firexapp.engine.default_celery_config import FireXAppCeleryConfig
from firexkit.task import FireXTask
from firexkit.firex_worker import FireXCeleryInspector, FireXWorkerId, NoCeleryResponse, \
    RunWorkerId
from firexapp.plugins import PLUGINS_ENV_NAME, PluginLoadError, get_active_plugins
from firexapp.reporters.json_reporter import FireXRunData
from firexapp.common import wait_until
from firexapp.submit.uid import Uid
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.broker_manager.redis_manager import RedisManager

logger = get_task_logger(__name__)

DEFAULT_CELERY_SHUTDOWN_TIMEOUT = 5 * 60


class FireXCelery(Celery):

    def __init__(
        self,
        *args,
        task_cls=f'{FireXTask.__module__}:{FireXTask.__name__}',
        fx_celery_config: typing.Optional[FireXAppCeleryConfig]=None,
        strict_typing=False,
        fx_discover_tasks=True,
        **kwargs,
    ):
        if fx_celery_config is None:
            fx_celery_config = FireXAppCeleryConfig(
                fx_discover_tasks=fx_discover_tasks,
            )
        super().__init__(
            *args,
            task_cls=task_cls,
            strict_typing=strict_typing,
            **kwargs,
        )
        self.fx_inspect = FireXCeleryInspector(self)


        self.steps['worker'].add(ReporterStep)
        self.steps['consumer'].add(BrokerShutdown)

        # We want this step to finish after Pool at least (because a poolworker writes this file in the async case),
        # but might as well finish after Hub too
        Hub.requires = Hub.requires + (ReporterStep,)

        self.config_from_object(fx_celery_config)

    @cached_property
    def AsyncResult(self):
        return self.subclass_with_self(
            'firexkit.result:FireXAsyncResult')

    @cached_property
    def FireXAsyncResult(self): # celery expects this???
        return self.subclass_with_self(
            'firexkit.result:FireXAsyncResult')

    @property
    def conf(self) -> FireXAppCeleryConfig:
        return super().conf

    def init_mc_conf(
        self,
        uid: Uid,
        soft_time_limit: typing.Optional[int],
    ):
        self.conf.set_run_var_from_env()
        self.backend.set('uid', str(uid))
        self.backend.set('logs_dir', uid.logs_dir)
        self.backend.set('resources_dir', uid.resources_dir)
        if soft_time_limit:
            self.backend.set('run_soft_time_limit', soft_time_limit)

    def get_mc_worker_id(self) -> FireXWorkerId:
        return FireXWorkerId.mc_id(hostname=self.conf.mc)

    def run_logs_dir(self) -> str:
        return self.conf.logs_dir

    def get_tasks_from_plugins(self):
        return [
            t for t in self.tasks.values() if t.from_plugin
        ]

    def import_services(
        self,
        plugins_files: typing.Union[None, str, list[str]]=None,
        imports: typing.Optional[tuple[str, ...]]=None,
    ) -> tuple[
        dict[str, FireXTask],
        dict[str, str],
    ]:
        original_plugins = _convert_plugins_to_list(plugins_files)
        resolved_plugins = self.cdl2list(original_plugins)

        # Create mapping from original plugin paths to resolved full paths
        plugin_path_mapping = {}
        # Build the mapping and validate files exist
        for original, resolved in zip(original_plugins, resolved_plugins):
            if not os.path.isfile(resolved):
                raise FileNotFoundError(resolved)
            plugin_path_mapping[original] = resolved

        if not imports:
            imports = self.conf.imports

        for module_name in imports:
            __import__(module_name)

        self._load_plugin_modules(resolved_plugins)

        return self.tasks, plugin_path_mapping

    def _load_plugin_modules(self, abs_plugins: list[str]):
        _update_plugins_env(abs_plugins)

        imported_module_names : list[str] = []
        new_tasks_long_names : set[str] = set()
        previous_tasks : set[str] = set(self.tasks)
        for plugin_file in abs_plugins:
            current_tasks = set(self.tasks)
            mod = self.import_plugin_file(plugin_file, replace=False)
            mod_new_tasks = current_tasks - previous_tasks
            new_tasks_long_names.update(mod_new_tasks)
            mod_new_tasks_modules = list({
                t.rsplit('.', 1)[0] for t in mod_new_tasks
            })

            file = mod.__file__ if mod else plugin_file
            print(f'{len(mod_new_tasks)} new service{"s" if len(mod_new_tasks)>1 else ""} '
                f'imported from plugin modules {mod_new_tasks_modules or ""}'
                f' found in {file}')

            imported_module_names += mod_new_tasks_modules
            previous_tasks = current_tasks

        if len(abs_plugins) > 1:
            if imported_module_names:
                print(f'--> {len(new_tasks_long_names)} total new service{"s" if len(new_tasks_long_names)>1 else ""} imported '
                    f'from {len(imported_module_names)} plugin module{"s" if len(imported_module_names)>1 else ""} '
                    f'{imported_module_names}')
            else:
                print(f'No new services imported from {abs_plugins}!')

        # Mark the newly imported tasks with "from_plugin"
        for long_name in new_tasks_long_names:
            self.tasks[long_name].from_plugin = True
        if abs_plugins:
            _unregister_duplicate_tasks(
                self,
                [_get_plugin_module_name(p) for p in abs_plugins],
                self.extra_copy_task_attrs(),
            )

        return new_tasks_long_names

    def import_plugin_file(self, plugin_file: str, replace: bool) -> typing.Optional[ModuleType]:
        plugin_file = self.find_plugin_file(plugin_file)
        module_name = _get_plugin_module_name(plugin_file)
        should_import, existing_module = _should_import(
            module_name,
            plugin_file,
            replace)
        if should_import:
            return _import_plugin(module_name, plugin_file)
        return existing_module

    @classmethod
    def extra_copy_task_attrs(cls) -> tuple[str, ...]:
        return tuple()

    def get_tasks_by_names(self, tasks: typing.Union[list[str], str]):
        return self.get_app_tasks(tasks, self.tasks)

    def get_task(self, task_short_name: str):
        return FireXCelery.get_app_task(
            task_short_name,
            self.tasks
        )

    def norm_chain_task_names(self, chain: typing.Union[list[str], str]) -> list[str]:
        try:
            return [t.short_name for t in self.get_tasks_by_names(chain)]
        except celery.exceptions.NotRegistered:
            if isinstance(chain, str):
                chain = chain.split(',')
            return [s.strip().split('.')[-1] for s in chain]

    @classmethod
    def get_app_tasks(
        cls,
        tasks: typing.Union[list[str], str],
        all_tasks: typing.Optional[dict],
    ):
        if isinstance(tasks, str):
            tasks = tasks.split(",")
        return [cls.get_app_task(t, all_tasks) for t in tasks]

    @staticmethod
    def get_app_task(task_short_name: str, all_tasks: typing.Optional[dict]):
        task_short_name = task_short_name.strip()
        if all_tasks is None:
            from firexapp.engine.celery import app
            all_tasks = app.tasks

        # maybe it isn't a short name, but a long one
        if task_short_name in all_tasks:
            return all_tasks[task_short_name]

        # Search for an exact match first
        for key, value in all_tasks.items():
            if key.split('.')[-1] == task_short_name:
                return value

        # Let's do a case-insensitive search
        task_name_lower = task_short_name.lower()
        for key, value in all_tasks.items():
            if key.split('.')[-1].lower() == task_name_lower:
                return value

        # Can't find a match
        raise celery.exceptions.NotRegistered(task_short_name)

    @classmethod
    def find_plugin_file(cls, file_path: str, rel_ok=True) -> str:
        # is it a full path?
        if os.path.isabs(file_path):
            plugin_file = file_path
        elif rel_ok:
            # Maybe it's relative?
            plugin_file = os.path.abspath(file_path)
        else:
            plugin_file = None

        if plugin_file:
            if os.path.isfile(plugin_file):
                return plugin_file
        raise FileNotFoundError(file_path)

    @classmethod
    def resolve_abs_plugins(
        cls,
        plugins: typing.Union[None, str, list[str]],
    ) -> list[str]:
        return cls.cdl2list(_convert_plugins_to_list(plugins))

    @classmethod
    def cdl2list(cls, plugin_files: list[str]) -> list[str]:
        return [
            cls.find_plugin_file(file)
            for file in plugin_files
            if file
        ]

    @classmethod
    def create_ut_fx_celery(cls, extra_module: typing.Optional[str]=None) -> 'FireXCelery':
        fx_app =  FireXCelery(
            set_as_current=False,
            fx_celery_config=FireXAppCeleryConfig(
                fx_discover_tasks=False,
                extra_imports=(extra_module,) if extra_module else tuple()
                ),
        )
        return fx_app

    def cancel_worker_id_queues_and_revoke_tasks(self, worker_id: FireXWorkerId):
        if worker_id.is_master():
            shutdown_queue_name = worker_id.as_shutdown_queue()
        else:
            shutdown_queue_name = None
        queues = [
            q.name for q in self.fx_inspect.get_single_worker_active_queues(worker_id)
            # need to keep shutdown queue for early release
            if q.name != shutdown_queue_name
        ]
        for queue in queues:
            self.control.cancel_consumer(
                queue=queue,
                destination=[str(worker_id)],
                reply=True,
            )

        _revoke_active_tasks(self, worker_ids=[worker_id])

    def is_celery_responsive(self) -> bool:
        from firexkit.inspect import inspect_with_retry
        return bool(
            inspect_with_retry(
                inspect_method='ping',
                celery_app=self,
                timeout=5,
                inspect_retry_timeout=4,
            )
        )

    def get_broker_mngr(self) -> RedisManager:
        return BrokerFactory.broker_manager_from_logs_dir(
            self.run_logs_dir()
        )

    def shutdown_firex_run(self, celery_shutdown_timeout: int):
        ordered_workers = RunWorkerId.get_shutdown_ordered_worker_ids(
            self.run_logs_dir()
        )
        if self.is_celery_responsive():
            for worker_id in ordered_workers:
                self.shutdown_firex_worker(worker_id)
        elif RunWorkerId.find_celery_procs_by_logs_dir(self.run_logs_dir()):
            logger.info("Celery not active, but found celery processes to force shutdown.")
            RunWorkerId.terminate_localhost_pidfiles(self.run_logs_dir(), timeout=60)
            if ordered_workers:
                for worker_id in ordered_workers:
                    self.shutdown_firex_worker(worker_id)
            else:
                logger.info("No Celery pidfiles found.")
        else:
            logger.info("No active Celery processes.")


    def shutdown_firex_worker(
        self,
        run_worker_id: RunWorkerId,
        cancel_queues=True,
        shutdown_timeout=DEFAULT_CELERY_SHUTDOWN_TIMEOUT,
    ):
        worker_id = run_worker_id.worker_id
        if cancel_queues:
            self.cancel_worker_id_queues_and_revoke_tasks(worker_id)

        self.control.shutdown(
            destination=[str(worker_id)],
            # mc shutdown kills the broker which can prevent the reply.
            reply=not worker_id.is_mc(),
        )
        no_celery_procs = wait_until(
            lambda: not run_worker_id.find_celery_procs_by_pidfile_cmdline(),
            timeout=shutdown_timeout,
            sleep_for=1,
        )
        if not no_celery_procs:
            if worker_id.on_cur_host():
                logger.warning(f"Celery not shutdown after {shutdown_timeout} secs, force killing instead.")
                run_worker_id.terminate_pid_file(timeout=60)
            else:
                logger.warning(f'Celery not shutdown after {shutdown_timeout} secs, cannot terminate procs on remote host {worker_id.hostname}')
        else:
            logger.debug("Confirmed Celery shutdown successfully.")


import time

def _revoke_active_tasks(
    fx_app: FireXCelery,
    worker_ids: typing.Optional[typing.Iterable[FireXWorkerId]]=None,
    max_revoke_retries=5,
    sleep_between_revoke_retries=1,
    task_predicate=lambda task: True,
):

    fx_app.fx_inspect.get_active_tasks(worker_ids=worker_ids)
    logger.debug("Querying Celery to find any remaining active tasks.")
    revoke_retries = 0
    try:
        active_tasks = fx_app.fx_inspect.get_active_tasks(worker_ids=worker_ids)
        # Revoke retry loop
        while active_tasks and revoke_retries < max_revoke_retries:

            if revoke_retries:
                logger.warning(f"Found {len(active_tasks)} active tasks after revoke. Revoking active tasks again.")
                time.sleep(sleep_between_revoke_retries)

            # Revoke tasks in order they were started. This avoids ChainRevokedException errors when children are revoked
            # before their parents.
            for task in sorted(active_tasks, key=lambda t: t.time_start or float('inf')):
                logger.info(f"Revoking {task.name}[{task.id}]")
                fx_app.control.revoke(task_id=task.id, terminate=True)

            if revoke_retries:
                logger.warning(f"Found {len(active_tasks)} active tasks after revoke. Revoking active tasks again.")

            # wait for confirmation of revoke
            active_tasks = fx_app.fx_inspect.get_active_tasks(worker_ids=worker_ids)
            wait_for_task_revoke_start = time.monotonic()
            while (
                active_tasks
                and time.monotonic() - wait_for_task_revoke_start < 3
            ):
                time.sleep(0.25)
                active_tasks = fx_app.fx_inspect.get_active_tasks(worker_ids=worker_ids)

            revoke_retries += 1
    except NoCeleryResponse:
        logger.info("Failed to read active tasks from celery. May shutdown with unrevoked tasks.")
    else:
        if not active_tasks:
            logger.info("Confirmed no active tasks after revoke.")
        else:
            logger.warning(
                f"Exceeded max revoke retry attempts, {len(active_tasks)} active tasks may not be revoked."
            )


import importlib.util

def _import_plugin(module_name, plugin_file) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, plugin_file)
    assert spec, f'No import spec from {plugin_file}'
    module = importlib.util.module_from_spec(spec)
    module_directory = os.path.dirname(os.path.realpath(plugin_file))
    if module_directory not in sys.path:
        sys.path.append(module_directory)
    mod = sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException as e:
        logger.exception(f'Failed to load {plugin_file}')
        raise PluginLoadError(f'Fatal Error loading plugin {plugin_file!r}') from e
    return mod


def _should_import(
    module_name: str,
    plugin_file: str,
    replace: bool,
) -> tuple[bool, typing.Optional[ModuleType]]:
    already_loaded_mod = None
    if module_name in sys.modules:
        # a module with this name is already loaded. See if we should replace it.
        existing_mod = sys.modules[module_name]
        module_source = existing_mod.__file__
        if module_source != plugin_file:
            if not replace:
                print(f'Plugin module {module_name!r} was NOT imported from {plugin_file!r}. '
                             f'A module with the same name was already imported from {module_source!r}')
                should_import = False
            else:
                print(
                    f'Plugin module {module_name!r} already loaded from {module_source!r}. '
                    f'Will replace with module from {plugin_file!r}'
                )
                should_import = True
        else:
            print(f'Plugin module {module_name!r} was already imported from {module_source!r}.')
            should_import = False
            already_loaded_mod = existing_mod
    else:
        should_import = True # new module name, always import.

    return should_import, already_loaded_mod


def _get_plugin_module_name(plugin_file: str) -> str:
    return os.path.splitext(os.path.basename(plugin_file))[0]


def _update_plugins_env(abs_plugins: list[str]) -> list[str]:
    env_plugins = [
        p for p in os.environ.get(PLUGINS_ENV_NAME, '').split(',')
        if p
    ]
    for new_p in abs_plugins:
        if new_p not in env_plugins:
            env_plugins.append(new_p)
    os.environ[PLUGINS_ENV_NAME] = ",".join(env_plugins)
    return env_plugins


from firexkit.task import REPLACEMENT_TASK_NAME_POSTFIX


def _unregister_duplicate_tasks(
    fx_app: FireXCelery,
    plugin_modules: list[str],
    extra_copy_attr_names: typing.Iterable[str],
):
    sigs = _get_signals_with_connections()
    many_long_names = _identify_duplicate_tasks(fx_app.tasks, plugin_modules)
    for plugin_long_names in many_long_names:
        prime_overrider = plugin_long_names[-1]  # the last item in the list is the last override
        for index in range(0, len(plugin_long_names)-1):
            overridden_name = plugin_long_names[index]
            new_task = _create_replacement_task(
                fx_app,
                fx_app.tasks[overridden_name],
                REPLACEMENT_TASK_NAME_POSTFIX * (len(plugin_long_names) - index - 1),
                sigs,
                extra_copy_attr_names,
            )
            fx_app.tasks[overridden_name] = fx_app.tasks[prime_overrider]
            replacement_name = plugin_long_names[index+1]
            fx_app.tasks[replacement_name].orig = new_task


# there is no way of copying the signals without coupling with the internals of celery signals
# noinspection PyProtectedMember
def _get_signals_with_connections():
    from celery.utils.dispatch.signal import Signal, NONE_ID
    # get all official signals
    signals = [s for s in celery.signals.__dict__.values() if type(s) is Signal]
    # only use the ones registered to specific microservices (as opposed to sender=None)
    signals = [s for s in signals if len(s.receivers) > len(s._live_receivers(None))]

    # now get the task specific registrations
    def from_sender_only(sig):
        return [k for k in sig.receivers if k[0][1] != NONE_ID]
    signals = {s: from_sender_only(s) for s in signals}
    signals = {s: k for s, k in signals.items() if k}

    return signals


def _identify_duplicate_tasks(
    all_tasks_long_names: typing.Iterable[str],
    priority_modules: list[str],
) -> list[list[str]]:
    """
    Returns a list of substitution. Each substitution is a list of microservices. The last will be the 'dominant' one.
    It will be the one used.
    """
    short_names_to_long_names: dict[str, list[str]] = {}
    for long_name in all_tasks_long_names:
        short_name = long_name.split('.')[-1]
        if short_name not in short_names_to_long_names:
            short_names_to_long_names[short_name] = []
        short_names_to_long_names[short_name].append(long_name)

    def priority_index(task_long_name):
        try:
            return priority_modules.index(os.path.splitext(task_long_name)[0])
        except ValueError:
            return -1

    return [
        sorted(long_names, key=priority_index)
        for long_names in short_names_to_long_names.values()
        if len(long_names) > 1
    ]


def _create_replacement_task(
    fx_app: FireXCelery,
    original: FireXTask,
    name_postfix: str,
    sigs,
    extra_copy_attr_names: typing.Iterable[str],
) -> FireXTask:
    new_name = original.name + name_postfix
    bound = inspect.ismethod(original.undecorated)
    func = original.run if not bound else original.run.__func__
    options = {
        key: getattr(original, key)
        for key in [
            "acks_late",
            "default_retry_delay",
            "expires",
            "ignore_result",
            "max_retries",
            "reject_on_worker_lost",
            "resultrepr_maxsize",
            "soft_time_limit",
            "store_errors_even_if_ignored",
            "soft_time_limit",
            "time_limit",
            "track_started",
            "trail",
            "typing",
            "returns",
            "flame",
            "use_cache",
            "pending_child_strategy",
            "from_plugin",
        ]
        if key in dir(original)
    }
    new_task = fx_app.task(
        name=new_name,
        bind=bound,
        base=inspect.getmro(original.__class__)[1],
        check_name_for_override_posfix=False,
        **options,
    )(fun=func)

    for attr_name in ["orig", "report_meta"] + list(extra_copy_attr_names):
        if hasattr(original, attr_name):
            setattr(
                new_task,
                attr_name,
                getattr(original, attr_name),
            )

    try:
        # there is no way of copying the signals without coupling with the internals of celery signals
        # noinspection PyProtectedMember
        from celery.utils.dispatch.signal import _make_id
        orig_task_id = _make_id(original)
        for s, receivers in sigs.items():
            for r in receivers:
                # format is ((id(receiver), id(sender)), ref(receiver))
                # locate any registered signal against the original microservice
                if r[0][1] == orig_task_id:
                    # new entry only replaces the
                    entry = ((r[0][0], _make_id(new_task)), r[1])
                    s.receivers.append(entry)
    except Exception as e:
        logger.error(f"Unable to copy signals while overriding {original.name}:\n{e}")
    return new_task


def _convert_plugins_to_list(plugin_files: typing.Union[None, str, list[str]]) -> list[str]:
    if not plugin_files:
        return []

    if not isinstance(plugin_files, list):
        plugin_files = [
            file.strip() for file in plugin_files.split(",")
            if file.strip()
        ]

    return plugin_files


class ReporterStep(bootsteps.StartStopStep):

    def include_if(self, parent):
        return FireXWorkerId.is_mc_worker_id(parent.hostname)

    def __init__(self, parent, **kwargs):
        self._logs_dir = None
        logfile = os.path.normpath(kwargs.get('logfile', '') or '')

        while (sp := os.path.split(logfile))[0] != logfile:
            m = FIREX_ID_REGEX.search(sp[1])
            if m:
                self._logs_dir = logfile
                break
            logfile = sp[0]

        super().__init__(parent, **kwargs)

    def stop(self, parent):
        # By now, the report should have been written! Write a default completion report
        if self._logs_dir:
            FireXRunData.set_revoked_if_incomplete(
                logs_dir=self._logs_dir,
                shutdown_reason='Celery stop bootstep unexpectedly found incomplete run',
            )

class BrokerShutdown(bootsteps.StartStopStep):
    """ This celery shutdown step will cleanup redis """
    label = "Broker"

    # noinspection PyMethodMayBeStatic
    def shutdown(self, parent):
        print(parent.app)
        if FireXWorkerId.is_mc_worker_id(parent.hostname):
            parent.app.get_broker_mngr().shutdown()
            logger.debug("Broker shut down from boot step.")
        else:
            logger.debug("Not the primary celery instance. Broker will not be shut down.")


# noinspection PyUnusedLocal
@celery.signals.worker_init.connect
def _worker_init_signal(sender, *args, **kwargs):
    try:
        sender.app.import_services(plugins_files=get_active_plugins())
    except PluginLoadError:
        traceback.print_exc()
        # We will just exit, the pid file won't be written,
        # and the bringup of the worker will eventually timeout
        exit(-2)