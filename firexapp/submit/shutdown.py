import os
import argparse
import logging
import subprocess
import time
from psutil import Process, TimeoutExpired

from celery import Celery

from firexapp.celery_manager import CeleryManager
from firexapp.submit.uid import Uid
from firexapp.broker_manager.broker_factory import BrokerFactory, REDIS_BIN_ENV
from firexkit.inspect import get_active
from firexapp.common import qualify_firex_bin, select_env_vars

logger = logging.getLogger(__name__)


def launch_background_shutdown(logs_dir):
    try:
        pid = subprocess.Popen([qualify_firex_bin("firex_shutdown"), "--logs_dir",  logs_dir],
                               close_fds=True, env=select_env_vars([REDIS_BIN_ENV, 'PATH'])).pid
    except Exception as e:
        logger.error("SHUTDOWN PROCESS FAILED TO LAUNCH -- REDIS WILL LEAK.")
        logger.error(e)
        raise
    else:
        try:
            Process(pid).wait(0.1)
        except TimeoutExpired:
            logger.debug("Started background shutdown with pid %s" % pid)
        else:
            logger.error("SHUTDOWN PROCESS FAILED TO RUN -- REDIS WILL LEAK.")


def wait_for_broker_shutdown(broker, timeout=15, force_kill=True):
    logger.debug("Waiting for broker to shut down")
    shutdown_wait_time = time.time() + timeout
    while time.time() < shutdown_wait_time:
        if not broker.is_alive():
            break
        time.sleep(0.1)

    if not broker.is_alive():
        logger.debug("Confirmed successful graceful broker shutdown.")
    elif force_kill:
        logger.debug("Warning! Broker was not shut down after %s seconds. FORCE KILLING BROKER." % str(timeout))
        broker.force_kill()

    return not broker.is_alive()



def get_active_broker_safe(broker, celery_app):
    #
    # Note: get_active can hang in celery/kombu library if broker is down.
    # Checking for broker.is_alive() is an attempt to prevent that, but note
    # the broker can theoretically die between that call and the get_active() call.
    if not broker.is_alive():
        return None
    return get_active(inspect_retry_timeout=2, celery_app=celery_app)


def _tasks_from_active(active):
    if not active:
        return []
    tasks = []
    for host in active.values():
        tasks.extend([t for t in host])
    return tasks


def revoke_active_tasks(broker, celery_app,  max_revoke_retries=5, task_predicate=lambda task: True):
    active_tasks = _tasks_from_active(get_active_broker_safe(broker, celery_app))
    revoke_retries = 0
    while active_tasks and revoke_retries < max_revoke_retries:
        if revoke_retries:
            logger.warning("Found %s active tasks after revoke. Revoking active tasks again." % len(active_tasks))

        for task in active_tasks:
            if task_predicate(task):
                logger.info(f"Revoking {task['name']}[{task['id']}]")
                celery_app.control.revoke(task_id=task["id"], terminate=True)

        time.sleep(2)
        active_tasks = [t for t in _tasks_from_active(get_active_broker_safe(broker, celery_app))
                        if task_predicate(t)]
        revoke_retries += 1

    if len(active_tasks) == 0:
        logger.info("Confirmed no active tasks after revoke.")
    elif revoke_retries >= max_revoke_retries:
        logger.warning("Exceeded max revoke retry attempts, %s active tasks may not be revoked."
                       % len(active_tasks))


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs_dir", help="Logs directory for the firexapp run to shutdown.",
                        required=True)
    args = parser.parse_args()
    logs_dir = args.logs_dir

    log_file = os.path.join(logs_dir, Uid.debug_dirname, 'shutdown.log')
    logging.basicConfig(filename=log_file, level=logging.DEBUG,
                        format='[%(asctime)s %(levelname)s] %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    return logs_dir


def shutdown_run(logs_dir):
    logger.info(f"Shutting down with logs: {logs_dir}.")
    broker = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
    logger.info(f"Shutting down with broker: {broker.broker_url}.")

    celery_manager = CeleryManager(logs_dir=logs_dir, broker=broker)
    celery_app = Celery(broker=broker.broker_url)

    try:
        if get_active_broker_safe(broker, celery_app):
            revoke_active_tasks(broker, celery_app)

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
    finally:
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
    try:
        shutdown_run(logs_dir)
    except Exception as e:
        logger.exception(e)
