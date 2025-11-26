import os
import traceback
from argparse import ArgumentParser, Action
from typing import Optional, Union, Iterable

from celery.utils.log import get_task_logger
from firexapp.common import delimit2list


logger = get_task_logger(__name__)
PLUGINS_ENV_NAME = "firex_plugins"


class PluginLoadError(Exception):
    pass


def plugins_has(plugins: Union[str, list[str]], query_basename: str) -> bool:
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
