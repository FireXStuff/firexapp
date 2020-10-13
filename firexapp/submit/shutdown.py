import os
import argparse
import logging
import subprocess
import time
from psutil import Process, TimeoutExpired, NoSuchProcess, pid_exists
from collections import namedtuple
import socket

from celery import Celery

from firexapp.celery_manager import CeleryManager
from firexapp.submit.uid import Uid
from firexapp.broker_manager.broker_factory import BrokerFactory, REDIS_BIN_ENV
from firexkit.inspect import get_active
from firexapp.common import qualify_firex_bin, select_env_vars

logger = logging.getLogger(__name__)


CELERY_SHUTDOWN_WAIT = 5 * 60
MaybeCeleryActiveTasks = namedtuple('MaybeCeleryActiveTasks', ['celery_read_success', 'active_tasks'])

def launch_background_shutdown(logs_dir, reason):
    try:
        shutdown_cmd = [qualify_firex_bin("firex_shutdown"), "--logs_dir",  logs_dir]
        if reason:
            shutdown_cmd += ['--reason', reason]
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



def get_active_broker_safe(broker, celery_app=None):
    if celery_app is None:
        celery_app = Celery(broker=broker.broker_url)

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


def _split_hostname(hostname):
    if '@' in hostname:
        return hostname.split('@')[-1]
    return hostname


def revoke_active_tasks(broker, celery_app,  max_revoke_retries=1, task_predicate=lambda task: True):
    maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker), task_predicate)
    revoked_worker_hosts_and_pids = set()
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
            logger.info(f"Revoking {task['name']}[{task['id']}] (pid {task['worker_pid']} on {task['hostname']})")
            celery_app.control.revoke(task_id=task["id"], terminate=True)
            revoked_worker_hosts_and_pids.add((_split_hostname(task['hostname']), task['worker_pid']))

        time.sleep(3)
        maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker), task_predicate)
        revoke_retries += 1

    if not maybe_active_tasks.celery_read_success:
        logger.info("Failed to read active tasks from celery. May shutdown with unrevoked tasks.")
    elif len(maybe_active_tasks.active_tasks) == 0:
        logger.info("Confirmed no active tasks after revoke.")
    elif revoke_retries >= max_revoke_retries:
        logger.warning("Exceeded max revoke retry attempts, %s active tasks may not be revoked."
                       % len(maybe_active_tasks.active_tasks))
    return revoked_worker_hosts_and_pids


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs_dir", help="Logs directory for the firexapp run to shutdown.",
                        required=True)
    parser.add_argument("--reason", help="A reason that will be logged for clarity.",
                        required=False, default='No reason provided.')
    args = parser.parse_args()
    logs_dir = args.logs_dir

    log_file = os.path.join(logs_dir, Uid.debug_dirname, 'shutdown.log')
    logging.basicConfig(filename=log_file, level=logging.DEBUG,
                        format='[%(asctime)s %(levelname)s] %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    return logs_dir, args.reason


def _is_current_host(other_host, cur_host):
    try:
        return socket.gethostbyname(other_host) == cur_host
    except socket.error:
        return False


def _wait_on_worker_pids(worker_hosts_and_pids, max_wait):
    current_host = socket.gethostbyname(socket.gethostname())

    cur_host_worker_pids = []

    for worker_pid_and_host in worker_hosts_and_pids:
        worker_host = worker_pid_and_host[0]
        worker_pid = worker_pid_and_host[1]
        if _is_current_host(worker_host, current_host):
            cur_host_worker_pids.append(worker_pid)
            try:
                Process(worker_pid).wait(max_wait)
            except NoSuchProcess:
                pass
            except TimeoutExpired:
                # TODO capture task UUID and name for logging purpose.
                logger.warning(f'Max timeout exceeded waiting for {worker_pid}.')
            else:
                logger.debug(f"Confirmed worker process {worker_pid} has terminated.")
        else:
            logger.warning(f"Waiting on remote worker pids is currently not supported, will not wait for: "
                           f"{worker_pid_and_host}")

    return all(not pid_exists(p) for p in cur_host_worker_pids)


def shutdown_run(logs_dir, reason='No reason provided'):
    logger.info(f"Shutting down due to reason: {reason}")
    logger.info(f"Shutting down with logs: {logs_dir}.")
    broker = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
    logger.info(f"Shutting down with broker: {broker.broker_url_safe_print}.")

    celery_manager = CeleryManager(logs_dir=logs_dir, broker=broker)
    celery_app = Celery(broker=broker.broker_url)

    try:
        if get_active_broker_safe(broker, celery_app):
            revoked_worker_hosts_and_pids = revoke_active_tasks(broker, celery_app)
            # all_local_pids_complete = _wait_on_worker_pids(revoked_worker_hosts_and_pids, max_wait=60)
            # logger.info(f"{'Confirmed' if all_local_pids_complete else 'Not'} all host-local revoked "
            #              f"task processes have terminated.")

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
    logs_dir, reason = init()
    try:
        shutdown_run(logs_dir, reason)
    except Exception as e:
        logger.exception(e)
