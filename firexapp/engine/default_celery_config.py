from celery.utils.log import get_task_logger

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.engine.logging import add_hostname_to_log_records, add_custom_log_levels, PRINT_LEVEL_NAME
from firexapp.discovery import find_firex_task_bundles

add_custom_log_levels()
add_hostname_to_log_records()

# logging formats
timestamp_format = "<small>[%(asctime)s]"
process_format = "[%(levelname)s/%(processName)-13s]"
task_format = "[%(task_id).8s-%(task_name)s]"
message_format = ":</small> %(message)s"
worker_log_format = timestamp_format + process_format + message_format
worker_task_log_format = timestamp_format + process_format + task_format + message_format

logger = get_task_logger(__name__)


broker_url = BrokerFactory.get_broker_url()
result_backend = broker_url

# find default tasks
logger.debug("Beginning bundle discovery")
bundles = find_firex_task_bundles()
logger.debug("Bundle discovery completed.")
if bundles:
    logger.debug('Bundles discovered:\n' + '\n'.join([f'\t - {b}' for b in bundles]))

# Plugins are imported via firexapp.plugins._worker_init_signal()
imports = tuple(bundles) + tuple(["firexapp.tasks.example",
                                  "firexapp.tasks.core_tasks",
                                  "firexapp.tasks.root_tasks",
                                  "firexapp.submit.report_trigger",
                                  "firexapp.reporters.json_reporter"
                                  ])

root_task = "firexapp.tasks.root_tasks.RootTask"

accept_content = ['pickle', 'json']
task_serializer = 'pickle'
result_serializer = 'pickle'
result_expires = None

task_track_started = True
task_acks_late = True

worker_prefetch_multiplier = 1

worker_redirect_stdouts_level = PRINT_LEVEL_NAME

primary_worker_name = 'mc'
primary_worker_minimum_concurrency = 4
mc = BrokerFactory.get_hostname_port_from_url(broker_url)[0]
