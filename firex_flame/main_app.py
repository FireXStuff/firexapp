import logging
import os
from threading import Thread

import celery
from firexapp.broker_manager.broker_factory import RedisManager

from firex_flame.controller import FlameAppController
from firex_flame.event_file_processor import process_recording_file
from firex_flame.event_broker_processor import BrokerEventConsumerThread
from firex_flame.flame_helper import wait_until_path_exist, FlameServerConfig

logger = logging.getLogger(__name__)


def create_broker_consumer_thread(broker_consumer_config, controller,
                                  recording_file, shutdown_handler, logs_dir):
    try:
        broker_url = RedisManager.get_broker_url_from_logs_dir(logs_dir)
    except:
        logger.error(f"Failed to load broker URL from logs dir: {logs_dir}")
        raise
    else:
        celery_app = celery.Celery(broker=broker_url, backend=broker_url)
        return BrokerEventConsumerThread(celery_app,
                                         controller,
                                         broker_consumer_config,
                                         recording_file,
                                         shutdown_handler)


def start_flame(server_config: FlameServerConfig, broker_consumer_config, run_metadata, shutdown_handler, wait_for_webserver):
    events_from_recording_file = server_config.recording_file and os.path.isfile(server_config.recording_file)
    controller = FlameAppController(
        run_metadata,
        server_config.extra_task_dump_paths,
        # Don't dump model if we are replaying from a recording file.
        dump_model=not events_from_recording_file,
    )
    if not events_from_recording_file:
        event_recv_thread = create_broker_consumer_thread(
            broker_consumer_config,
            controller,
            server_config.recording_file,
            shutdown_handler,
            run_metadata['logs_dir'])
        celery_app = event_recv_thread.celery_app
    else:
        event_recv_thread = Thread(target=process_recording_file, args=(controller, server_config.recording_file))
        celery_app = None
    event_recv_thread.start()

    # If the client (e.g. main firex process) isn't waiting for the web server and it is waiting for the
    # receiver to be ready, speed up startup by waiting for the receiver to be ready before loading
    # any web modules.
    if (
        not wait_for_webserver
        and not events_from_recording_file
        and broker_consumer_config.receiver_ready_file
    ):
        wait_until_path_exist(broker_consumer_config.receiver_ready_file, sleep_for=0.1)

    # Delaying of importing of all web dependencies is a deliberate startup performance optimization.
    # The broker should be listening for events as quickly as possible.
    from firex_flame.web_app import start_web_server #noqa
    return start_web_server(server_config, controller, celery_app)
