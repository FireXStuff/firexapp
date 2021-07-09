import os
import argparse
import logging
import subprocess
import time
from psutil import Process, TimeoutExpired
from collections import namedtuple

from celery import Celery
import redis.exceptions
import kombu.exceptions

from firexapp.celery_manager import CeleryManager
from firexapp.submit.uid import Uid
from firexapp.broker_manager.broker_factory import BrokerFactory, REDIS_BIN_ENV
from firexkit.inspect import get_active, get_revoked, get_active_queues
from firexapp.common import qualify_firex_bin, select_env_vars

logger = logging.getLogger(__name__)


DEFAULT_CELERY_SHUTDOWN_TIMEOUT = 5 * 60
MaybeCeleryActiveTasks = namedtuple('MaybeCeleryActiveTasks', ['celery_read_success', 'active_tasks'])


def launch_background_shutdown(logs_dir, reason, celery_shutdown_timeout=DEFAULT_CELERY_SHUTDOWN_TIMEOUT):
    try:
        shutdown_cmd = [qualify_firex_bin("firex_shutdown"),
                        "--logs_dir",  logs_dir,
                        "--celery_shutdown_timeout", str(celery_shutdown_timeout)]
        if reason:
            shutdown_cmd += ['--reason', reason]
        pid = subprocess.Popen(shutdown_cmd,
                               close_fds=True,
                               env=select_env_vars([REDIS_BIN_ENV, 'PATH']),
                               preexec_fn=os.setpgrp,
                               ).pid
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


def _inspect_broker_safe(inspect_fn, broker, celery_app, **kwargs):
    #
    # Note: get_active can hang in celery/kombu library if broker is down.
    # Checking for broker.is_alive() is an attempt to prevent that, but note
    # the broker can theoretically die between that call and the get_active() call.
    if not broker.is_alive():
        return None
    try:
        return inspect_fn(inspect_retry_timeout=4, celery_app=celery_app, **kwargs)
    except (redis.exceptions.ConnectionError, kombu.exceptions.DecodeError) as e:
        logger.warning(e)
        return None


def get_active_broker_safe(broker, celery_app):
    return _inspect_broker_safe(get_active, broker, celery_app,
                                # shutdown can't deserialize task args because it doesn't know about many classes
                                # (e.g. FireXUid), so set active(safe=True)
                                method_args=(True,))


def is_celery_responsive(broker, celery_app):
    # use get_active_queues instead of get_active to check Celery responsiveness
    # because get_active can fail to deserialize its response (i.e. raise DecodeError)
    return _inspect_broker_safe(get_active_queues, broker, celery_app)


def get_revoked_broker_safe(broker, celery_app):
    if not broker.is_alive():
        return None
    return get_revoked(inspect_retry_timeout=4, celery_app=celery_app)


def _tasks_from_active(active, task_predicate) -> MaybeCeleryActiveTasks:
    if active is None:
        return MaybeCeleryActiveTasks(False, None)
    tasks = []
    for host in active.values():
        tasks.extend([t for t in host if task_predicate(t)])
    return MaybeCeleryActiveTasks(True, tasks)


def revoke_active_tasks(broker, celery_app,  max_revoke_retries=5, task_predicate=lambda task: True):
    logger.debug("Querying Celery to find any remaining active tasks.")
    maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker, celery_app), task_predicate)
    revoke_retries = 0
    # Revoke retry loop
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

        # wait for confirmation of revoke
        maybe_active_tasks = _tasks_from_active(get_active_broker_safe(broker, celery_app), task_predicate)
        wait_for_task_revoke_start = time.monotonic()
        while maybe_active_tasks.celery_read_success and maybe_active_tasks.active_tasks \
                and time.monotonic() - wait_for_task_revoke_start < 3:
            time.sleep(0.25)
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
    parser.add_argument("--celery_shutdown_timeout", help="Timeout in seconds for which to wait for Celery shutdown before terminating Celery processes individually."
                        " Celery will wait for task completion after receiving a graceful shutdown, so this timeout should consider how "
                        "long FireX shutdown should be willing to wait for remaining tasks.",
                        default=DEFAULT_CELERY_SHUTDOWN_TIMEOUT, type=int)
    args = parser.parse_args()
    logs_dir = args.logs_dir

    log_file = os.path.join(logs_dir, Uid.debug_dirname, 'shutdown.log.txt')
    logging.basicConfig(filename=log_file, level=logging.DEBUG,
                        format='[%(asctime)s %(levelname)s] %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")

    return logs_dir, args.reason, args.celery_shutdown_timeout


def shutdown_run(logs_dir, celery_shutdown_timeout, reason='No reason provided'):
    logger.info(f"Shutting down due to reason: {reason}")
    logger.info(f"Shutting down with logs: {logs_dir}.")
    broker = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
    logger.info(f"Shutting down with broker: {broker.broker_url_safe_print}.")

    celery_manager = CeleryManager(logs_dir=logs_dir, broker=broker)
    celery_app = Celery(broker=broker.broker_url,
                        accept_content=['pickle', 'json'])

    try:
        if is_celery_responsive(broker, celery_app):
            revoke_active_tasks(broker, celery_app)

            logger.info("Found active Celery; sending Celery shutdown.")
            celery_app.control.shutdown()

            celery_shutdown_success = celery_manager.wait_for_shutdown(celery_shutdown_timeout)
            if not celery_shutdown_success:
                logger.warning(f"Celery not shutdown after {celery_shutdown_timeout} secs, force killing instead.")
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
            broker.shutdown()
            wait_for_broker_shutdown(broker)
        else:
            logger.info("No active Broker.")

    logger.info("Shutdown of %s complete." % logs_dir)


def main():
    logs_dir, reason, celery_shutdown_timeout = init()
    try:
        shutdown_run(logs_dir, celery_shutdown_timeout, reason)
    except Exception as e:
        logger.exception(e)
