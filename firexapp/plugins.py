import importlib.util
import inspect
import os
import sys
from argparse import Action, ArgumentParser
from types import ModuleType

from celery.utils.log import get_task_logger

from firexapp.common import delimit2list
from firexkit.firexkit_common import REPLACEMENT_TASK_NAME_POSTFIX

logger = get_task_logger(__name__)

PLUGINS_ENV_NAME = "firex_plugins"


class PluginLoadError(Exception):
    pass


def plugins_has(plugins: str | list[str], query_basename: str) -> bool:
    """Check if a plugin basename is present in the plugins string or list.

    Args:
        plugins: Either a comma-separated string of plugin paths or a list of plugin paths
        query_basename: The basename of the plugin to search for (e.g., 'sparse_build.py')

    Returns:
        True if the query_basename is found in plugins, False otherwise
    """
    if isinstance(plugins, list):
        # Handle list of plugins
        return any(
            plugin == query_basename or plugin.endswith(f'/{query_basename}')
            for plugin in plugins
        )
    return plugins == query_basename or plugins.endswith(f'/{query_basename}')


def _get_short_name(long_name: str) -> str:
    return long_name.split('.')[-1]


def convert_plugins_to_list(plugin_files: None | str | list[str]) -> list[str]:
    if not plugin_files:
        return []

    if not isinstance(plugin_files, list):
        plugin_files = [
            file.strip() for file in plugin_files.split(",")
        ]

    return plugin_files


def _get_plugin_module_name(plugin_file):
    return os.path.splitext(os.path.basename(plugin_file))[0]


# there is no way of copying the signals without coupling with the internals of celery signals
# noinspection PyProtectedMember
def _get_signals_with_connections():
    import celery.signals as sigs
    from celery.utils.dispatch.signal import NONE_ID, Signal
    # get all official signals
    signals = [s for s in sigs.__dict__.values() if type(s) is Signal]
    # only use the ones registered to specific microservices (as opposed to sender=None)
    signals = [s for s in signals if len(s.receivers) > len(s._live_receivers(None))]

    # now get the task specific registrations
    def from_sender_only(sig):
        return [k for k in sig.receivers if k[0][1] != NONE_ID]
    signals = {s: from_sender_only(s) for s in signals}
    signals = {s: k for s, k in signals.items() if k}

    return signals


class FxPluginRegistry:

    def create_replacement_task(
        self,
        fx_app,
        original,
        name_postfix,
        sigs,
    ):
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
                "pydantic_validate", # FIXME: shouldn't need to duplicate.
            ]
            if key in dir(original)
        }
        new_task = fx_app.task(
            name=new_name,
            bind=bound,
            base=inspect.getmro(original.__class__)[1],
            check_name_for_override_posfix=False,
            **options
        )(fun=func)

        new_task.orig = getattr(original, "orig", None)
        if hasattr(original, "report_meta"):
            new_task.report_meta = original.report_meta

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
            logger.error("Unable to copy signals while overriding %s:\n%s" % (original.name, str(e)))
        return new_task

    @classmethod
    def import_plugin_file(
        cls,
        plugin_file: str,
        replace=False,
    ) -> ModuleType | None:

        plugin_file = cls.find_plugin_file(plugin_file)
        module_name = _get_plugin_module_name(plugin_file)
        should_import, existing_module = _should_import(
            module_name,
            plugin_file,
            replace,
        )
        if should_import:
            return _import_plugin(module_name, plugin_file)

        return existing_module

    def _unregister_duplicate_tasks(
        self,
        fx_app,
        imported_module_names: list[str],
    ):
        sigs = _get_signals_with_connections()
        overridden_short_names_to_long = _identify_duplicate_tasks(
            # Registration order matters: it's the tie-breaker for tasks whose
            # modules have equal priority, so this must not become a set.
            list(fx_app.tasks),
            imported_module_names,
        )
        for single_task_long_names in overridden_short_names_to_long.values():
            final_long_name = single_task_long_names[-1]
            for index in range(len(single_task_long_names) - 1):
                original_name = single_task_long_names[index]
                original_task = fx_app.tasks[original_name]
                fx_app.tasks[original_name] = fx_app.tasks[final_long_name]

                new_task = self.create_replacement_task(
                    fx_app,
                    original_task,
                    name_postfix=REPLACEMENT_TASK_NAME_POSTFIX * (len(single_task_long_names) - index - 1),
                    sigs=sigs,
                )
                overrider = single_task_long_names[index+1]
                fx_app.tasks[overrider].orig = new_task

    @classmethod
    def _import_plugin_files(
        cls,
        fx_app,
        plugin_files: str | list[str]
    ) -> list[str]:
        # Modules that contribute overriding (i.e. plugin) tasks, in increasing
        # order of priority. This is not limited to the modules backing the
        # plugin files themselves: a plugin file very commonly just imports the
        # module that actually defines the overriding tasks, and those tasks
        # must take priority over the tasks they override just the same.
        plugin_module_names: list[str] = []
        # Names of every module backing a plugin file in this call,
        # regardless of whether importing it actually executed fresh
        # code (see below for why this is tracked independently of
        # new_tasks).
        plugin_file_module_names: list[str] = []
        if plugin_files := cls.resolve_plugin_paths(plugin_files):
            new_tasks : set[str] = set()

            for plugin_file in plugin_files:
                pre_import_task_names = set(fx_app.tasks)
                mod = cls.import_plugin_file(plugin_file)
                if mod is not None:
                    plugin_file_module_names.append(mod.__name__)

                    # Kept in registration order, since that's what breaks
                    # priority ties between duplicate task names.
                    new_task_names : list[str] = [
                        t for t in fx_app.tasks if t not in pre_import_task_names
                    ]
                    if new_task_names:
                        new_tasks.update(new_task_names)
                        new_tasks_modules_from_this_import = list(
                            dict.fromkeys(
                                t.rsplit('.', 1)[0] for t in new_task_names
                            )
                        )
                        plugin_modules_info = f'{new_tasks_modules_from_this_import} '
                    else:
                        new_tasks_modules_from_this_import = []
                        plugin_modules_info = ''

                    # The plugin file's own module is appended last so that it
                    # outranks the modules it imported: a plugin file that both
                    # imports and redefines a task must win over the import.
                    for module_name in [
                        m for m in new_tasks_modules_from_this_import
                        if m != mod.__name__
                    ] + [mod.__name__]:
                        if module_name not in plugin_module_names:
                            plugin_module_names.append(module_name)

                    logger.info(
                        f'{len(new_task_names)} new service{"s" if len(new_task_names)>1 else ""} '
                        f'imported from plugin modules {plugin_modules_info}'
                        f'found in {mod.__file__ if mod else plugin_file}'
                    )

            if plugin_file_module_names:
                uniq_mods = len(set(plugin_file_module_names))
                logger.info(
                    f'--> {len(new_tasks)} total new service{"s" if len(new_tasks)>1 else ""} imported '
                    f'from {uniq_mods} plugin module{"s" if uniq_mods > 1 else ""} '
                    f'{plugin_file_module_names}')
            else:
                logger.info(f'No new services imported from {plugin_files}!')

            # Mark tasks defined by any of these plugin modules with
            # "from_plugin". This intentionally does NOT rely solely on
            # new_tasks (tasks that appeared during this call's own
            # pre/post diff): a plugin module can end up already imported
            # (e.g. as a transitive dependency of another plugin/module
            # processed earlier, or via an earlier, separate call to plugin
            # loading) by the time its own turn in this loop is reached, in
            # which case import_plugin_file() returns the cached module
            # without re-executing it and none of its tasks show up as
            # "new" here -- even though they are still genuinely from this
            # plugin file and must be marked accordingly.
            for t, task in fx_app.tasks.items():
                if t in new_tasks or getattr(task, '__module__', None) in plugin_file_module_names:
                    task.from_plugin = True

        return plugin_module_names

    @classmethod
    def set_plugins_env(cls, plugin_files):
        os.environ[PLUGINS_ENV_NAME] = ",".join(
            cls.resolve_plugin_paths(plugin_files)
        )

    def load_plugin_modules(
        self,
        fx_app,
        plugin_files: str | list[str],
    ):
        self.set_plugins_env(plugin_files)
        imported_module_names = self._import_plugin_files(
            fx_app,
            plugin_files,
        )
        if imported_module_names:
            self._unregister_duplicate_tasks(fx_app, imported_module_names)

    @classmethod
    def find_plugin_file(cls, file_path: str) -> str:
        # is it a full path?
        if os.path.isabs(file_path):
            plugin_file = file_path
        else:
            # Maybe it's relative?
            plugin_file = os.path.abspath(file_path)
        if os.path.isfile(plugin_file):
            return plugin_file
        raise FileNotFoundError(file_path)

    @classmethod
    def resolve_plugin_paths(
        cls,
        plugin_files: None | str | list[str],
    ) -> list[str]:
        return [
            cls.find_plugin_file(file)
            for file in convert_plugins_to_list(plugin_files)
            if file
        ]


def _identify_duplicate_tasks(
    all_task_long_names: list[str],
    new_plugin_module_names: list[str],
) -> dict[str, list[str]]:
    """
    Returns the long names of duplicately named tasks, keyed by their short (i.e. duplicated) name.
    Each value is ordered by increasing priority, so the last entry is the 'dominant' one: it will be
    the one used. Tasks from modules absent from 'new_plugin_module_names' have the lowest priority,
    and ties are broken by the order of 'all_task_long_names' (i.e. registration order).
    """
    short_names_to_lone_names : dict[str, list[str]] = {
        _get_short_name(long_name): []
        for long_name in all_task_long_names
    }
    for long_name in all_task_long_names:
        short_names_to_lone_names[
            _get_short_name(long_name)
        ].append(long_name)

    def priority_index(long_task_name):
        try:
            return new_plugin_module_names.index(
                os.path.splitext(long_task_name)[0]
            )
        except ValueError:
            return -1

    long_task_names_by_basenames = {
        short_name: sorted(long_names, key=priority_index)
        for short_name, long_names in short_names_to_lone_names.items()
        if len(long_names) > 1
    }
    return long_task_names_by_basenames


def _should_import(
    module_name: str,
    plugin_file: str,
    replace: bool,
) -> tuple[bool, ModuleType | None]:
    already_loaded = None
    if module_name in sys.modules:
        # a module with this name is already loaded. See if we should replace it.
        existing_mod = sys.modules[module_name]
        module_source = existing_mod.__file__
        if module_source != plugin_file:
            if not replace:
                logger.error(f'Plugin module {module_name!r} was NOT imported from {plugin_file!r}. '
                             f'A module with the same name was already imported from {module_source!r}')
                should_import = False
            else:
                logger.warning(
                    f'Plugin module {module_name!r} already loaded from {module_source!r}. '
                    f'Will replace with module from {plugin_file!r}'
                )
                should_import = True
        else:
            logger.warning(f'Plugin module {module_name!r} was already imported from {module_source!r}.')
            should_import = False
            already_loaded = existing_mod
    else:
        # new module name, always import.
        should_import = True

    return should_import, already_loaded


def _import_plugin(module_name, plugin_file):
    spec = importlib.util.spec_from_file_location(module_name, plugin_file)
    module = importlib.util.module_from_spec(spec)
    module_directory = os.path.dirname(os.path.realpath(plugin_file))
    if module_directory not in sys.path:
        sys.path.append(module_directory)
    mod = sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        logger.exception(f'Failed to load {plugin_file}')
        raise PluginLoadError(f'Fatal Error loading plugin {plugin_file!r}')
    return mod


def get_active_plugins() -> str:
    return os.environ.get(PLUGINS_ENV_NAME, "")


def merge_plugins(*plugin_lists) -> list[str]:
    """Merge  comma delimited lists of plugins into a single list. Right-handed most significant plugin"""
    combined_list = []
    for plugin_list in plugin_lists:
        combined_list += delimit2list(plugin_list)
    new_list = []
    for next_idx, plugin in enumerate(combined_list, start=1):
        if plugin not in combined_list[next_idx:]:
            new_list.append(plugin)
    return new_list


class CommaDelimitedListAction(Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        self.is_default = True
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        old_value = getattr(namespace, self.dest) if hasattr(namespace, self.dest) and not self.is_default else ""
        self.is_default = False
        if old_value:
            old_value += ","
        new_value = ",".join(merge_plugins(old_value, values))
        setattr(namespace, self.dest, new_value)


plugin_support_parser = ArgumentParser(add_help=False)
plugin_support_parser.add_argument(
    "--external", "--plugins", '-external', '-plugins', "--plugin",
    help="Comma delimited list of plugins files to load",
    default="",
    dest='plugins',
    action=CommaDelimitedListAction,
)
