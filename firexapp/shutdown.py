import os
import argparse
import logging
import subprocess
import time

from celery import Celery

from firexapp.celery_manager import CeleryManager
from firexapp.submit.uid import Uid
from firexapp.broker_manager.broker_factory import BrokerFactory
from firexkit.inspect import get_active
from firexapp.common import qualify_firex_bin


logger = logging.getLogger(__name__)


def launch_background_shutdown(logs_dir):
    subprocess.Popen([qualify_firex_bin("firex_shutdown"), "--logs_dir",  logs_dir], close_fds=True)


def wait_for_broker_shutdown(broker, timeout=15):
    logger.debug("Waiting for broker to shut down")
    shutdown_wait_time = time.time() + timeout
    while time.time() < shutdown_wait_time:
        if not broker.is_alive():
            break
        time.sleep(0.1)

    if broker.is_alive():
        logger.debug("Warning! Broker was not shut down after %s seconds. FORCE KILLING BROKER." % str(timeout))
        broker.force_kill()
    else:
        logger.debug("Confirmed successful graceful broker shutdown.")


def revoke_active_tasks(celery_app):
    revoked = []
    active = get_active(celery_app=celery_app)
    if active:
        for host in active.values():
            if host:
                logger.info('Revoking lingering tasks...')
            for task in host:
                revoked.append(task["id"])
                logger.warning("Revoking " + task['name'])
                celery_app.control.revoke(task_id=task["id"], terminate=True)
    return revoked


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs_dir", help="Logs directory for the firexapp run to shutdown.",
                        required=True)
    args = parser.parse_args()
    logs_dir = args.logs_dir

    log_file = os.path.join(logs_dir, Uid.debug_dirname, 'shutdown.log')
    logging.basicConfig(filename=log_file, level=logging.DEBUG, filemode='w',
                        format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    return logs_dir


def shutdown_run(logs_dir):
    celery_manager = CeleryManager(logs_dir=logs_dir)
    broker = BrokerFactory.get_broker_manager(logs_dir=logs_dir)
    celery_app = Celery(broker=broker.broker_url)

    if get_active(inspect_retry_timeout=2, celery_app=celery_app):
        revoke_active_tasks(celery_app)

        logger.info("Found active Celery; sending Celery shutdown.")
        celery_app.control.shutdown()

        celery_shutdown_wait = 15
        celery_shutdown_success = celery_manager.wait_for_shutdown(celery_shutdown_wait)
        if not celery_shutdown_success:
            logger.warning("Celery not shutdown after %d secs, force killing instead." % celery_shutdown_wait)
            celery_manager.shutdown()
        else:
            logger.debug("Confirmed Celery shutdown successfully.")
    elif celery_manager.find_all_procs():
        logger.info("Celery not active, but found celery processes to force shutdown.")
        celery_manager.shutdown()
    else:
        logger.info("No active Celery processes.")

    if broker.is_alive():
        logger.info("Broker is alive; sending redis shutdown.")
        # broker will be shut down by celery if active
        broker.shutdown()
        wait_for_broker_shutdown(broker)
    else:
        logger.info("No active Broker.")

    logger.info("Shutdown of %s complete." % logs_dir)


def main():
    logs_dir = init()
    shutdown_run(logs_dir)
