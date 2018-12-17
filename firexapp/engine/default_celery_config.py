
from firexapp.plugins import get_plugin_module_list
from firexapp.discovery import find_firex_task_bundles

bundles = find_firex_task_bundles()
imports = tuple(bundles) + tuple(get_plugin_module_list())
