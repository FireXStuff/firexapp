from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.plugins import get_plugin_module_list
from firexapp.discovery import find_firex_task_bundles


broker_url = BrokerFactory.get_broker_url()
result_backend = broker_url

bundles = find_firex_task_bundles()
imports = tuple(bundles) + tuple(get_plugin_module_list())
