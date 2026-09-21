import dataclasses
import hashlib
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


@dataclasses.dataclass(frozen=True)
class PluginModules:
    """
        The modules a single plugin file contributed tasks from.

        A plugin file very commonly just imports the module that actually defines the
        overriding tasks, so a plugin is a *group* of modules, not only the module
        backing the plugin file itself.

        'task_module_names' is ordered by increasing priority and always ends with
        'module_name', so that a plugin file that both imports and redefines a task
        wins over the module it imported.

        'task_long_names' is what the plugin actually defined, and is deliberately
        kept separate from 'task_module_names': a module's name outlives any single
        plugin import, so tasks registered into one of these modules *after* the
        plugin was loaded are not part of the plugin and must not be treated as
        group-local.

        'plugin_file_hash' identifies the plugin by content rather than by path, so
        that the same plugin reached through two different absolute paths is
        recognised as one plugin. See is_same_plugin().
    """
    plugin_file: str
    module_name: str
    task_module_names: tuple[str, ...]
    task_long_names: frozenset[str] = frozenset()
    plugin_file_hash: str | None = None

    def is_same_plugin(self, other: 'PluginModules') -> bool:
        """
            Whether both groups came from what is really the same plugin.

            The same plugin is routinely reachable through more than one absolute
            path -- firex_cisco's ci_plugins, for instance, ships both in
            site-packages and in the workspace -- so identical module name plus
            identical file content means identical plugin, even when the paths differ.
        """
        if self.plugin_file == other.plugin_file:
            return True
        return (
            self.module_name == other.module_name
            and self.plugin_file_hash is not None
            and self.plugin_file_hash == other.plugin_file_hash
        )


def _hash_file(file_path: str | None) -> str | None:
    """Content hash of a file, or None when it can't be read."""
    if not file_path:
        return None
    try:
        with open(file_path, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        # Unreadable or not a regular file. Callers treat an unknown hash as
        # "not known to be identical", which keeps the cautious behaviour.
        return None


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


def _get_module_name(long_task_name: str) -> str:
    return long_task_name.rsplit('.', 1)[0]


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

    def __init__(self):
        # Track all loaded plugin groups so that _unregister_duplicate_tasks can
        # compute group dominants correctly across multiple load_plugin_modules calls.
        self._loaded_plugin_groups: list[PluginModules] = []

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
                "plugin_local_override",
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
        plugin_groups: list[PluginModules],
    ):
        sigs = _get_signals_with_connections()
        # The tasks plugins actually *defined*. Keyed on the task long names captured
        # while a plugin was imported rather than on module name, because a module's
        # name outlives any single plugin import: tasks registered into one of a
        # plugin's modules afterwards were not defined by the plugin.
        #
        # Deliberately a flat set rather than a per-plugin-group mapping. Resolution
        # is per *defining module*, and a plugin group routinely spans several
        # modules: a plugin file that imports another plugin's module (e.g.
        # 'from nxpidt.nxospibringup_slurm import config_ixia_license') pulls that
        # module's tasks into its own group, so resolving per group would make the
        # importing plugin's task displace the imported module's same-named task --
        # exactly the collision this is meant to prevent. Since a given module
        # contributes at most one long name per short name, per-module resolution
        # means a plugin-defined name simply always resolves to itself.
        plugin_defined_long_names = {
            long_name
            for group in plugin_groups
            for long_name in group.task_long_names
        }

        overridden_short_names_to_long = _identify_duplicate_tasks(
            # Registration order matters: it's the tie-breaker for tasks whose
            # modules have equal priority, so this must not become a set.
            list(fx_app.tasks),
            _priority_module_names(plugin_groups),
        )
        for single_task_long_names in overridden_short_names_to_long.values():
            final_long_name = single_task_long_names[-1]

            for index in range(len(single_task_long_names) - 1):
                original_name = single_task_long_names[index]
                original_task = fx_app.tasks[original_name]

                if original_name not in plugin_defined_long_names:
                    # Core code, or a task registered into a plugin's module after the
                    # plugin loaded: the highest-precedence plugin wins, as before.
                    fx_app.tasks[original_name] = fx_app.tasks[final_long_name]
                # else: defined by a plugin, so the module that defined it keeps
                # reaching its own version. Overriding is matched on short name only,
                # so two unrelated plugins that both define e.g. 'checkout_git_branch'
                # would otherwise silently run each other's code. Nothing outside that
                # module refers to this long name -- core code and the CLI chain
                # resolve the short name to the dominant.

                new_task = self.create_replacement_task(
                    fx_app,
                    original_task,
                    name_postfix=REPLACEMENT_TASK_NAME_POSTFIX * (len(single_task_long_names) - index - 1),
                    sigs=sigs,
                )
                overrider = single_task_long_names[index+1]
                fx_app.tasks[overrider].orig = new_task

            for long_name in single_task_long_names[:-1]:
                if long_name in plugin_defined_long_names:
                    # Reachable only from its own module: everywhere else this short
                    # name resolves to the global dominant. apply_async consults this
                    # so the task isn't republished under the name it overrides, which
                    # the worker would resolve back to the dominant.
                    fx_app.tasks[long_name].plugin_local_override = True

    @classmethod
    def _is_core_module(cls, fx_app, module_name: str) -> bool:
        """
            Whether a module is the app's own code rather than a plugin's. A plugin
            file that merely happens to be the first thing to import a core task
            module must not hand that core module plugin precedence, or the core
            implementation would outrank a genuine override from a lower-precedence
            plugin.

            Asked one module at a time, rather than by fetching a set of core module
            names up front, because the modules this has to judge are exactly the ones
            a plugin import has only just pulled in: subclasses can therefore decide
            from the imported module itself (e.g. where its file is), which a snapshot
            taken before the import could not.
        """
        # Small enough (bundle modules) that a linear scan per candidate is cheaper
        # than building a set, and conf.imports is itself cached.
        return module_name in (getattr(fx_app.conf, 'imports', None) or ())

    @classmethod
    def _import_plugin_files(
        cls,
        fx_app,
        plugin_files: str | list[str],
        log_level: int,
    ) -> list[PluginModules]:
        # One group per plugin file that contributed tasks, in increasing order of
        # priority. A group is not limited to the module backing the plugin file
        # itself: a plugin file very commonly just imports the module that actually
        # defines the overriding tasks, and those tasks must take priority over the
        # tasks they override just the same.
        plugin_groups: list[PluginModules] = []
        # Every module already claimed by a group, so that a module imported by two
        # plugin files keeps the priority of the first (i.e. least significant) one.
        claimed_module_names: set[str] = set()
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
                    new_tasks.update(new_task_names)
                    new_tasks_modules_from_this_import = list(
                        dict.fromkeys(
                            _get_module_name(t) for t in new_task_names
                        )
                    )

                    # The plugin file's own module is appended last so that it
                    # outranks the modules it imported: a plugin file that both
                    # imports and redefines a task must win over the import.
                    group_module_names = tuple(
                        m for m in [
                            m for m in new_tasks_modules_from_this_import
                            if m != mod.__name__ and not cls._is_core_module(fx_app, m)
                        ] + [mod.__name__]
                        if m not in claimed_module_names
                    )
                    if group_module_names:
                        claimed_module_names.update(group_module_names)
                        # Restricted to the group's own modules: core modules and
                        # modules already claimed by a lower-precedence plugin also
                        # register tasks during this import, and those tasks are not
                        # this plugin's to own -- treating them as plugin-defined
                        # would exempt them from being overridden at all.
                        group_task_long_names = {
                            t for t in new_task_names
                            if _get_module_name(t) in group_module_names
                        }
                        if not group_task_long_names:
                            # import_plugin_file() returned an already-imported module
                            # without re-executing it, so the pre/post diff is empty.
                            # Fall back to the tasks currently registered under this
                            # group's modules, otherwise a second load of the same
                            # plugin file would own nothing and group-local resolution
                            # would silently do nothing.
                            group_task_long_names = {
                                t for t in fx_app.tasks
                                if _get_module_name(t) in group_module_names
                            }
                        plugin_groups.append(
                            PluginModules(
                                plugin_file=plugin_file,
                                module_name=mod.__name__,
                                task_module_names=group_module_names,
                                task_long_names=frozenset(group_task_long_names),
                                plugin_file_hash=_hash_file(plugin_file),
                            )
                        )

                    plugin_modules_info = (
                        f'{list(group_module_names)} ' if group_module_names else ''
                    )
                    logger.log(
                        log_level,
                        f'{len(new_task_names)} new service{"s" if len(new_task_names)>1 else ""} '
                        f'imported from plugin modules {plugin_modules_info}'
                        f'found in {mod.__file__ if mod else plugin_file}',
                    )

            if plugin_file_module_names:
                uniq_mods = len(set(plugin_file_module_names))
                logger.log(
                    log_level,
                    f'--> {len(new_tasks)} total new service{"s" if len(new_tasks)>1 else ""} imported '
                    f'from {uniq_mods} plugin module{"s" if uniq_mods > 1 else ""} '
                    f'{plugin_file_module_names}')
            else:
                logger.log(log_level, f'No new services imported from {plugin_files}!')

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

        return plugin_groups

    @classmethod
    def set_plugins_env(cls, plugin_files):
        os.environ[PLUGINS_ENV_NAME] = ",".join(
            cls.resolve_plugin_paths(plugin_files)
        )

    def load_plugin_modules(
        self,
        fx_app,
        plugin_files: str | list[str],
        log_level: int,
    ):
        self.set_plugins_env(plugin_files)
        plugin_groups = self._import_plugin_files(
            fx_app,
            plugin_files,
            log_level,
        )
        if plugin_groups:
            # _unregister_duplicate_tasks must see every group loaded so far, not just
            # this call's: otherwise a plugin loaded by an earlier call is invisible
            # when a later, higher-precedence plugin arrives, and both would keep
            # their own version of a shared short name.
            for group in plugin_groups:
                existing = next(
                    (
                        i for i, g in enumerate(self._loaded_plugin_groups)
                        if g.is_same_plugin(group)
                    ),
                    None,
                )
                if existing is None:
                    self._loaded_plugin_groups.append(group)
                else:
                    # Reloading a plugin file must refresh what it owns without
                    # changing where it sits in the precedence order.
                    self._loaded_plugin_groups[existing] = group
            self._unregister_duplicate_tasks(fx_app, self._loaded_plugin_groups)

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


def _priority_module_names(plugin_groups: list[PluginModules]) -> list[str]:
    """Every plugin module, flattened in increasing order of priority."""
    return [
        module_name
        for group in plugin_groups
        for module_name in group.task_module_names
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
                _get_module_name(long_task_name)
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
        # Builtin and namespace modules have no __file__, so a plugin file whose
        # basename collides with one of those must not raise AttributeError here.
        module_source = getattr(existing_mod, '__file__', None)
        # Compare resolved paths so that the same file reached through a symlink
        # isn't mistaken for a different module of the same name.
        if (
            module_source is None
            or os.path.realpath(module_source) != os.path.realpath(plugin_file)
        ):
            if replace:
                logger.warning(
                    f'Plugin module {module_name!r} already loaded from {module_source!r}. '
                    f'Will replace with module from {plugin_file!r}'
                )
                should_import = True
            else:
                # This plugin file is being ignored in favour of the resident module,
                # but that only actually loses something when the two differ. The same
                # plugin is routinely reachable through more than one absolute path
                # (firex_cisco's ci_plugins ships both in site-packages and in the
                # workspace), and identical content means nothing was lost, so that
                # case is not worth an error.
                existing_hash = _hash_file(module_source)
                if existing_hash is not None and existing_hash == _hash_file(plugin_file):
                    logger.debug(
                        f'Plugin module {module_name!r} was already imported from '
                        f'{module_source!r}, which is identical in content to '
                        f'{plugin_file!r}.'
                    )
                    # Same code, so the resident module *is* this plugin: hand it back
                    # like the same-path case, otherwise the caller sees no module and
                    # the plugin contributes no tasks and no priority at all.
                    already_loaded = existing_mod
                else:
                    logger.error(f'Plugin module {module_name!r} was NOT imported from {plugin_file!r}. '
                                 f'A module with the same name was already imported from {module_source!r}')
                should_import = False
        else:
            # Literally the same file, so there is nothing to warn about: the caller
            # gets the resident module back and the plugin contributes exactly what it
            # would have. Re-loading a plugin that is already resident is routine --
            # script_plugins re-imports every preceding plugin in each forked child,
            # and a fork inherits the parent's sys.modules -- so this is noise.
            logger.debug(
                f'Plugin module {module_name!r} was already imported from {module_source!r}.')
            should_import = False
            already_loaded = existing_mod
    else:
        # new module name, always import.
        should_import = True

    return should_import, already_loaded


def _import_plugin(module_name: str, plugin_file: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, plugin_file)
    if spec is None or spec.loader is None:
        raise PluginLoadError(
            f'Cannot load plugin {plugin_file!r}: not an importable Python module.'
        )
    module = importlib.util.module_from_spec(spec)
    module_directory = os.path.dirname(os.path.realpath(plugin_file))
    if module_directory not in sys.path:
        sys.path.append(module_directory)

    previously_loaded = sys.modules.get(module_name)
    sys.modules[module_name] = module
    loaded = False
    try:
        spec.loader.exec_module(module)
        loaded = True
    except Exception as e:
        logger.exception(f'Failed to load {plugin_file}')
        raise PluginLoadError(f'Fatal Error loading plugin {plugin_file!r}') from e
    finally:
        if not loaded:
            # Don't leave a partially executed module behind: _should_import would
            # hand it to the next caller as though it had imported successfully.
            # Note this also runs for BaseExceptions (e.g. KeyboardInterrupt),
            # which are deliberately not converted to PluginLoadError.
            if previously_loaded is not None:
                sys.modules[module_name] = previously_loaded
            else:
                sys.modules.pop(module_name, None)
    # The module may have replaced itself in sys.modules.
    return sys.modules[module_name]


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
