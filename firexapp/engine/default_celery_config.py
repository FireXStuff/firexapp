import os

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.plugins import get_plugin_module_list
from firexapp.discovery import find_firex_task_bundles, discover_package_modules


broker_url = BrokerFactory.get_broker_url()
result_backend = broker_url

# find default tasks
default_tasks = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tasks")
bundles = find_firex_task_bundles()
imports = tuple(bundles) + tuple(get_plugin_module_list()) + tuple(discover_package_modules(default_tasks))

accept_content = ['pickle']
task_serializer = 'pickle'
result_serializer = 'pickle'

result_expires = None

task_acks_late = True
worker_prefetch_multiplier = 1
