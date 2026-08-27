import argparse
import logging
import os
import sys
import signal

# Prevent dependencies from taking module loading hit of pkg_resources.
sys.modules["pkg_resources"] = type('noop', (object,), {})

from celery.app.base import Celery

from firexapp.broker_manager.broker_factory import RedisManager
from firexapp.events.model import FireXRunMetadata
from firex_keeper.keeper_event_consumer import TaskDatabaseAggregatorThread
from firex_keeper.keeper_helper import get_keeper_dir

logger = logging.getLogger(__name__)


def celery_app_from_logs_dir(logs_dir):
    return Celery(broker=RedisManager.get_broker_url_from_logs_dir(logs_dir),
                  accept_content=['pickle', 'json'])


def _sig_handler(_, __):
    logger.warning("Received SIGTERM.")
    sys.exit(1)


def init_keeper():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs_dir", help="Logs directory for the run to keep task data for.",
                        required=True)
    parser.add_argument("--uid", help="FireX UID for the run to keep task data for.",
                        required=True)
    parser.add_argument("--chain", help="Logs directory for the run to keep task data for.",
                        required=True)
    parser.add_argument('--broker_recv_ready_file', help='File to create immediately before capturing celery events.',
                        default=None)

    args = parser.parse_args()

    run_metadata = FireXRunMetadata(args.uid, args.logs_dir, args.chain, None, firex_requester=None)

    keeper_dir = get_keeper_dir(run_metadata.logs_dir)
    os.makedirs(keeper_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(keeper_dir, 'keeper.log.txt'), level=logging.DEBUG, filemode='w',
                        format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    logger.info('Starting Keeper with args: %s' % args)

    signal.signal(signal.SIGTERM, _sig_handler)

    celery_app = celery_app_from_logs_dir(run_metadata.logs_dir)
    return celery_app, run_metadata, args.broker_recv_ready_file


def main():
    celery_app, run_metadata, receiver_ready_file = init_keeper()
    try:
        TaskDatabaseAggregatorThread(celery_app, run_metadata,
                                     receiver_ready_file=receiver_ready_file).run()
    except Exception as e:
        logger.error("Keeper process terminating due to error.")
        logger.exception(e)
        raise
    else:
        logger.info("Keeper process terminating gracefully.")
