from celery.utils.log import get_task_logger

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.engine.logging import add_hostname_to_log_records, add_custom_log_levels, PRINT_LEVEL_NAME
from firexapp.plugins import get_plugin_module_list
from firexapp.discovery import find_firex_task_bundles

add_custom_log_levels()
add_hostname_to_log_records()

logger = get_task_logger(__name__)


broker_url = BrokerFactory.get_broker_url()
result_backend = broker_url

# find default tasks
logger.debug("Beginning bundle discovery")
bundles = find_firex_task_bundles()
logger.debug("Bundle discovery completed")

imports = tuple(bundles) + tuple(get_plugin_module_list()) + tuple(["firexapp.tasks.example",
                                                                    "firexapp.tasks.core_tasks",
                                                                    "firexapp.submit.report_trigger",
                                                                    "firexapp.tasks.ci"])

root_task = "firexapp.tasks.core_tasks.RootTask"

accept_content = ['pickle']
task_serializer = 'pickle'
result_serializer = 'pickle'

result_expires = None

task_track_started = True
task_acks_late = True

worker_prefetch_multiplier = 1

worker_redirect_stdouts_level = PRINT_LEVEL_NAME
