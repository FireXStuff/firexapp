import os
import sys
import inspect
from argparse import ArgumentParser, Action

from celery.signals import worker_init
from celery.utils.log import get_task_logger
from firexapp.common import delimit2list
from firexkit.task import REPLACEMENT_TASK_NAME_POSTFIX
import importlib.util

logger = get_task_logger(__name__)
PLUGGING_ENV_NAME = "firex_plugins"


def get_short_name(long_name: str) -> str:
    return long_name.split('.')[-1]


def find_plugin_file(file_path):
    # is it a full path?
    if os.path.isabs(file_path):
        return file_path

    # is it relative to the cwd?
    in_cwd = os.path.abspath(file_path)
    if os.path.isfile(in_cwd):
        return in_cwd
    raise FileNotFoundError(file_path)


def cdl2list(plugin_files):
    if not plugin_files:
        return []

    plugin_files = [file.strip() for file in plugin_files.split(",")]
    plugin_files = [find_plugin_file(file) for file in plugin_files if file]
    return plugin_files


def get_plugin_module_name(plugin_file):
    return os.path.splitext(os.path.basename(plugin_file))[0]


def get_plugin_module_names(plugin_files):
    files = cdl2list(plugin_files)
    if not files:
        return []
    return [get_plugin_module_name(file) for file in files]


def get_plugin_module_names_from_env():
    plugin_files = get_active_plugins()
    return get_plugin_module_names(plugin_files)


# noinspection PyUnusedLocal
@worker_init.connect()
def _worker_init_signal(*args, **kwargs):
    load_plugin_modules_from_env()


def _mark_plugin_module_tasks():
    from celery import current_app
    ext_mods = get_plugin_module_names_from_env()
    for ext_mod in ext_mods:
        ext_mod_tasks = [t for t in current_app.tasks if t.startswith(ext_mod)]
        for ext_mod_task in ext_mod_tasks:
            current_app.tasks[ext_mod_task].from_plugin = True


# there is no way of copying the signals without coupling with the internals of celery signals
# noinspection PyProtectedMember
def _get_signals_with_connections():
    from celery.utils.dispatch.signal import Signal, NONE_ID
    import celery.signals as sigs
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


def create_replacement_task(original, name_postfix, sigs):
    new_name = original.name + name_postfix
    bound = inspect.ismethod(original.undecorated)
    func = original.run if not bound else original.run.__func__
    options = {key: getattr(original, key) for key in ["acks_late",
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
                                                       "pending_child_strategy"] if key in dir(original)}

    from celery import current_app
    new_task = current_app.task(name=new_name,
                                bind=bound,
                                base=inspect.getmro(original.__class__)[1],
                                check_name_for_override_posfix=False,
                                **options)(fun=func)
    if hasattr(original, "orig"):
        new_task.orig = original.orig
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


def _unregister_duplicate_tasks():
    sigs = _get_signals_with_connections()
    from celery import current_app
    becomes = identify_duplicate_tasks(current_app.tasks, get_plugin_module_names_from_env())
    for substitutions in becomes:
        prime_overrider = substitutions[-1]  # the last item in the list is the last override
        for index in range(0, len(substitutions)-1):
            original_name = substitutions[index]
            original = current_app.tasks[original_name]
            current_app.tasks[original_name] = current_app.tasks[prime_overrider]

            postfix = REPLACEMENT_TASK_NAME_POSTFIX * (len(substitutions) - index - 1)
            new_task = create_replacement_task(original, postfix, sigs)
            overrider = substitutions[index+1]
            current_app.tasks[overrider].orig = new_task


def identify_duplicate_tasks(all_tasks, priority_modules: list) -> [[]]:
    """
    Returns a list of substitution. Each substitution is a list of microservices. The last will be the 'dominant' one.
    It will be the one used.
    """
    unique_names = set([get_short_name(long_name) for long_name in all_tasks])
    unique_names = {name: list() for name in unique_names}
    for long_name in all_tasks:
        unique_names[get_short_name(long_name)].append(long_name)

    def priority_index(micro_name):
        try:
            return priority_modules.index(os.path.splitext(micro_name)[0])
        except ValueError:
            return -1
    overrides = [sorted(long_names, key=priority_index) for long_names in unique_names.values() if len(long_names) > 1]
    return overrides


def import_plugin_file(plugin_file):

    def _import_plugin(module_name, plugin_file):
        spec = importlib.util.spec_from_file_location(module_name, plugin_file)
        module = importlib.util.module_from_spec(spec)
        module_directory = os.path.dirname(os.path.abspath(plugin_file))
        if module_directory not in sys.path:
            sys.path.append(module_directory)
        mod = sys.modules[module_name] = module
        spec.loader.exec_module(module)
        print(f"Plugins module {module_name!r} imported from {plugin_file!r}")
        return mod

    plugin_file = find_plugin_file(plugin_file)
    module_name = get_plugin_module_name(plugin_file)

    if module_name in sys.modules:
        module_source = sys.modules[module_name].__file__
        if module_source != plugin_file:
            logger.error(f'Plugin module {module_name!r} was NOT imported from {plugin_file!r}. '
                         f'A module with the same name was already imported from {module_source!r}')
        else:
            logger.warning(f'Plugin module {module_name!r} was already imported from {module_source!r}.')
    else:
        return _import_plugin(module_name, plugin_file)


def import_plugin_files(plugin_files):
    plugin_files = cdl2list(plugin_files)
    if not plugin_files:
        return
    for plugin_file in plugin_files:
        import_plugin_file(plugin_file)


def set_plugins_env(plugin_files):
    plugin_files = cdl2list(plugin_files)
    os.environ[PLUGGING_ENV_NAME] = ",".join(plugin_files)


def get_active_plugins():
    return os.environ.get(PLUGGING_ENV_NAME, "")


def load_plugin_modules(plugin_files):
    set_plugins_env(plugin_files)
    import_plugin_files(plugin_files)
    if plugin_files:
        _unregister_duplicate_tasks()
        _mark_plugin_module_tasks()


def load_plugin_modules_from_env():
    plugin_files = get_active_plugins()
    load_plugin_modules(plugin_files)


def merge_plugins(*plugin_lists) -> []:
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
        super(CommaDelimitedListAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        old_value = getattr(namespace, self.dest) if hasattr(namespace, self.dest) and not self.is_default else ""
        self.is_default = False
        if old_value:
            old_value += ","
        new_value = ",".join(merge_plugins(old_value, values))
        setattr(namespace, self.dest, new_value)


plugin_support_parser = ArgumentParser(add_help=False)
plugin_support_parser.add_argument("--external", "--plugins", '-external', '-plugins', "--plugin",
                                   help="Comma delimited list of plugins files to load",
                                   default="",
                                   dest='plugins',
                                   action=CommaDelimitedListAction)
