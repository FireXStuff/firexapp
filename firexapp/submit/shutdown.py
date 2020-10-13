import os
import argparse
import logging
import subprocess
import time
from psutil import Process, TimeoutExpired, NoSuchProcess
from collections import namedtuple

from celery import Celery

from firexapp.celery_manager import CeleryManager
from firexapp.submit.uid import Uid
from firexapp.broker_manager.broker_factory import BrokerFactory, REDIS_BIN_ENV
from firexkit.inspect import get_active
from firexapp.common import qualify_firex_bin, select_env_vars

logger = logging.getLogger(__name__)


CELERY_SHUTDOWN_WAIT = 5 * 60
MaybeCeleryActiveTasks = namedtuple('MaybeCeleryActiveTasks', ['celery_read_success', 'active_tasks'])

def launch_background_shutdown(logs_dir, reason, root_task_name):
    try:
        shutdown_cmd = [qualify_firex_bin("firex_shutdown"), "--logs_dir",  logs_dir]
        if reason:
            shutdown_cmd += ['--reason', reason]
        if root_task_name:
            shutdown_cmd += ['--root_task_name', root_task_name]
        pid = subprocess.Popen(shutdown_cmd, close_fds=True, env=select_env_vars([REDIS_BIN_ENV, 'PATH'])).pid
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
    return get_active(inspect_retry_timeout=4, celery_app=celery_app)


def _tasks_from_active(active, task_predicate) -> MaybeCeleryActiveTasks:
    if active is None:
        return MaybeCeleryActiveTasks(False, None)
    tasks = []
    for host in active.values():
        tasks.extend([t for t in host if task_predicate(t)])
    return MaybeCeleryActiveTasks(True, tasks)


def _wait_on_task_worker_pids(tasks, max_wait):
    # FIXME: track total time spent waiting instead of sending max_wait to each process.
    for task in tasks:
        # FIXME: check task['hostname'] to avoid waiting on remote pids, or support waiting on remote pids.
        worker_pid = task['worker_pid']
        task_label = f'{task["name"]}[{task["id"]}] (pid {worker_pid})'
        logger.info(f"Waiting on {task_label}")
        try:
            Process(worker_pid).wait(max_wait)
        except NoSuchProcess:
            logger.debug(f"Confirmed worker process {task_label} terminated before wait.")
        except TimeoutExpired:
            logger.warning(f'Max timeout exceeded waiting for {task_label}.')
        else:
            logger.debug(f"Confirmed worker process {task_label} has terminated.")


def _wait_on_root(maybe_active_tasks, root_task_name, max_wait):
    # Under normal circumstances, the root completing (becoming failed, succeeded or revoked) causes shutdown.
    # The root task being complete is important to avoid shutdown terminating cleanup tasks.
    # However, somewhat inexplicably, sometimes the root is still active here during shutdown, causing us to revoke
    # it again, along with any cleanup tasks. We therefore try to wait for the root task pid to complete.
    if root_task_name and maybe_active_tasks.celery_read_success:
        root_tasks = [t for t in maybe_active_tasks.active_tasks if t['name'].endswith(root_task_name)]
        if root_tasks:
            _wait_on_task_worker_pids(root_tasks, max_wait)
            return True
    return False


def revoke_active_tasks(broker, celery_app,  max_revoke_retries=5, task_predicate=lambda task: True,
                        root_task_name=None):
    maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker, celery_app), task_predicate)
    found_root_tasks = _wait_on_root(maybe_active_tasks, root_task_name, max_wait=60)
    if found_root_tasks:
        # If we've found root tasks, we've waited, so the initial maybe_active_tasks are stale, so re-fetch.
        maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker, celery_app), task_predicate)

    revoke_retries = 0
    while (maybe_active_tasks.celery_read_success
           and maybe_active_tasks.active_tasks
           and revoke_retries < max_revoke_retries):
        if revoke_retries:
            logger.warning("Found %s active tasks after revoke. Revoking active tasks again."
                           % len(maybe_active_tasks.active_tasks))

        # Revoke tasks in order they were started. This avoids ChainRevokedException errors when children are revoked
        # before their parents.
        for task in sorted(maybe_active_tasks.active_tasks, key=lambda t: t.get('time_start', float('inf'))):
            logger.info(f"Revoking {task['name']}[{task['id']}]")
            celery_app.control.revoke(task_id=task["id"], terminate=True)

        time.sleep(3)
        maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker, celery_app), task_predicate)
        revoke_retries += 1

    if not maybe_active_tasks.celery_read_success:
        logger.info("Failed to read active tasks from celery. May shutdown with unrevoked tasks.")
    elif len(maybe_active_tasks.active_tasks) == 0:
        logger.info("Confirmed no active tasks after revoke.")
    elif revoke_retries >= max_revoke_retries:
        logger.warning("Exceeded max revoke retry attempts, %s active tasks may not be revoked."
                       % len(maybe_active_tasks.active_tasks))


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs_dir", help="Logs directory for the firexapp run to shutdown.",
                        required=True)
    parser.add_argument("--reason", help="A reason that will be logged for clarity.",
                        required=False, default='No reason provided.')
    parser.add_argument("--root_task_name", help="The name of the root task to avoid revoking.",
                        required=False, default=None)
    args = parser.parse_args()
    logs_dir = args.logs_dir

    log_file = os.path.join(logs_dir, Uid.debug_dirname, 'shutdown.log')
    logging.basicConfig(filename=log_file, level=logging.DEBUG,
                        format='[%(asctime)s %(levelname)s] %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    return logs_dir, args.reason, args.root_task_name


def shutdown_run(logs_dir, reason, root_task_name):
    logger.info(f"Shutting down due to reason: {reason}")
    logger.info(f"Shutting down with logs: {logs_dir}.")
    broker = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
    logger.info(f"Shutting down with broker: {broker.broker_url_safe_print}.")

    celery_manager = CeleryManager(logs_dir=logs_dir, broker=broker)
    celery_app = Celery(broker=broker.broker_url)

    try:
        if get_active_broker_safe(broker, celery_app):
            revoke_active_tasks(broker, celery_app, root_task_name=root_task_name)

            logger.info("Found active Celery; sending Celery shutdown.")
            celery_app.control.shutdown()

            celery_shutdown_success = celery_manager.wait_for_shutdown(CELERY_SHUTDOWN_WAIT)
            if not celery_shutdown_success:
                logger.warning(f"Celery not shutdown after {CELERY_SHUTDOWN_WAIT} secs, force killing instead.")
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
    logs_dir, reason, root_task_name = init()
    try:
        shutdown_run(logs_dir, reason, root_task_name)
    except Exception as e:
        logger.exception(e)
