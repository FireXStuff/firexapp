import argparse
import errno
import logging
import os
import subprocess
import tempfile
import time
from collections import namedtuple
from datetime import datetime

import kombu.exceptions
import redis.exceptions
from flufl.lock import Lock, TimeOutError
from psutil import Process, TimeoutExpired

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.celery_manager import CeleryManager
from firexapp.common import qualify_firex_bin
from firexapp.engine.default_celery_config import FxEnvVars
from firexapp.submit.uid import Uid
from firexkit.firex_celery import FireXCelery
from firexkit.inspect import get_active, ping
from firexkit.result import ManyFxAsyncResults

logger = logging.getLogger(__name__)


DEFAULT_CELERY_SHUTDOWN_TIMEOUT = 5 * 60
_DEFAULT_REVOKE_CLEANUP_TIMEOUT = 2 * 60
MaybeCeleryActiveTasks = namedtuple(
    "MaybeCeleryActiveTasks",
    ["celery_read_success", "active_tasks"],
)

# fork() fails with these while memory or process slots are briefly exhausted. A run
# ending under memory pressure is exactly when that happens, and giving up leaks the
# run's broker, celery workers and tracking services for as long as leak detection
# takes to notice them.
_TRANSIENT_LAUNCH_ERRNOS = frozenset({errno.ENOMEM, errno.EAGAIN})
_SHUTDOWN_LAUNCH_RETRY_DELAYS = (1, 2, 4, 8, 16)


def _launch_shutdown_subprocess(shutdown_cmd: list[str], logs_dir: str) -> int:
    shutdown_subprocess_env = FxEnvVars.select_minimal_fx_env_from_os_env()
    shutdown_cwd = logs_dir if os.path.isdir(logs_dir) else tempfile.gettempdir()
    try:
        import detach
    except ModuleNotFoundError:
        # don't break old installs that don't have detach

        return subprocess.Popen(
            shutdown_cmd,
            close_fds=True,
            env=shutdown_subprocess_env,
            start_new_session=True,
            # Shutdown doesn't care about cwd, but Celery can crash with bad cwd :/
            cwd=shutdown_cwd,
        ).pid
    else:
        return detach.call(
            shutdown_cmd,
            env=shutdown_subprocess_env,
            cwd=shutdown_cwd,
        )


def _launch_shutdown_subprocess_with_retries(
    shutdown_cmd: list[str], logs_dir: str
) -> int:
    for delay in _SHUTDOWN_LAUNCH_RETRY_DELAYS:
        try:
            return _launch_shutdown_subprocess(shutdown_cmd, logs_dir)
        except OSError as e:
            if e.errno not in _TRANSIENT_LAUNCH_ERRNOS:
                raise
            logger.warning(
                f"Failed to launch shutdown process, retrying in {delay}s: {e}"
            )
            time.sleep(delay)
    return _launch_shutdown_subprocess(shutdown_cmd, logs_dir)


def _run_shutdown_lock(logs_dir: str, *, lifetime: int | None = None) -> Lock:
    return Lock(
        os.path.join(logs_dir, Uid.debug_dirname, "shutdown.lock"), lifetime=lifetime
    )


def launch_background_shutdown(
    logs_dir: str,
    reason,
    celery_shutdown_timeout=DEFAULT_CELERY_SHUTDOWN_TIMEOUT,
) -> int | None:
    # The submit process may already have started shutdown (for example, on
    # SIGINT) and directly caused the root to enter its revoke postrun.
    try:
        if _run_shutdown_lock(logs_dir).expiration > datetime.now():
            logger.debug("Run shutdown already has an owner. Skipping launch.")
            return None
    except FileNotFoundError:
        pass

    try:
        shutdown_cmd = [
            qualify_firex_bin("firex_shutdown"),
            "--logs_dir",
            logs_dir,
            "--celery_shutdown_timeout",
            str(celery_shutdown_timeout),
        ]
        if reason:
            shutdown_cmd += ["--reason", reason]
        pid = _launch_shutdown_subprocess_with_retries(shutdown_cmd, logs_dir)
    except Exception:
        logger.exception("SHUTDOWN PROCESS FAILED TO LAUNCH -- REDIS WILL LEAK.")
        raise
    else:
        try:
            Process(pid).wait(0.1)
        except TimeoutExpired:
            logger.debug(f"Started background shutdown with pid {pid}")
            return pid
        else:
            logger.error("SHUTDOWN PROCESS FAILED TO RUN -- REDIS WILL LEAK.")
            return None


def _wait_for_broker_shutdown(broker, timeout=15, force_kill=True):
    logger.debug("Waiting for broker to shut down")
    shutdown_wait_time = time.time() + timeout
    while time.time() < shutdown_wait_time:
        if not broker.is_alive():
            break
        time.sleep(0.1)

    if not broker.is_alive():
        logger.debug("Confirmed successful graceful broker shutdown.")
    elif force_kill:
        logger.debug(
            f"Warning! Broker was not shut down after {timeout} seconds. FORCE KILLING BROKER."
        )
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
    except (
        redis.exceptions.ConnectionError,
        kombu.exceptions.DecodeError,
        kombu.exceptions.OperationalError,
    ) as ex:
        logger.warning(ex)
        return None


def get_active_broker_safe(broker, celery_app):
    return _inspect_broker_safe(
        get_active,
        broker,
        celery_app,
        # shutdown can't deserialize task args because it doesn't know about many classes
        # (e.g. FireXUid), so set active(safe=True)
        method_args=(True,),
    )


def is_celery_responsive(broker, celery_app) -> bool:
    # use ping instead of get_active to check Celery responsiveness
    # because get_active can fail to deserialize its response (i.e. raise DecodeError).
    # Need some celery call that uses broadcast, since we'll call shutdown that uses
    # broadcast.
    r = _inspect_broker_safe(ping, broker, celery_app)
    return bool(r)


def _tasks_from_active(active, task_predicate) -> MaybeCeleryActiveTasks:
    if active is None:
        return MaybeCeleryActiveTasks(False, None)
    tasks = []
    for host in active.values():
        tasks.extend([t for t in host if task_predicate(t)])
    return MaybeCeleryActiveTasks(True, tasks)


def revoke_active_tasks(
    broker,
    celery_app: FireXCelery,
    max_revoke_retries=5,
    task_predicate=None,
    *,
    cleanup_timeout: float = _DEFAULT_REVOKE_CLEANUP_TIMEOUT,
):
    logger.debug("Querying Celery to find any remaining active tasks.")
    matches = task_predicate or (lambda task: True)
    deadline = time.monotonic() + cleanup_timeout
    for attempt in range(max_revoke_retries + 1):
        maybe_active_tasks = _tasks_from_active(
            get_active_broker_safe(broker, celery_app), matches
        )
        if not maybe_active_tasks.celery_read_success:
            logger.info(
                "Failed to read active tasks from celery. May shutdown with unrevoked tasks."
            )
            return
        active_tasks = maybe_active_tasks.active_tasks

        # Already revoked tasks can be absent from inspect.active() while their
        # cleanup still needs workers and Redis. A targeted revoke only waits for
        # the tasks it selects.
        try:
            wait_on = (
                celery_app.get_running_task_ids() if task_predicate is None else set()
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger.warning("Lost the result backend while finding tasks to revoke.")
            return
        if not active_tasks and not wait_on:
            logger.info("Confirmed no active tasks or pending revoke cleanup.")
            return
        if attempt == max_revoke_retries:
            logger.warning("Exceeded max revoke retry attempts with unfinished tasks.")
            return
        # Revoke tasks in order they were started. This avoids ChainRevokedException errors when children are revoked
        # before their parents.
        for task in sorted(
            active_tasks, key=lambda t: t.get("time_start", float("inf"))
        ):
            logger.info(f"Revoking {task['name']}[{task['id']}]")
            celery_app.control.revoke(task_id=task["id"], terminate=True)
            wait_on.add(task["id"])

        results = ManyFxAsyncResults.fx_ars_from_list(
            celery_app.AsyncResult(task_id) for task_id in sorted(wait_on)
        )
        try:
            # Postrun follows event publication. Checking result state here
            # would also deserialize results from plugins this process hasn't loaded.
            if not results.wait_for_running(
                max_wait=max(0, deadline - time.monotonic()), query_timeout=2
            ):
                logger.warning("Continuing shutdown with unfinished task cleanup.")
                return
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger.warning("Lost the result backend while waiting for task cleanup.")
            return


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logs_dir",
        help="Logs directory for the firexapp run to shutdown.",
        required=True,
    )
    parser.add_argument(
        "--reason",
        help="A reason that will be logged for clarity.",
        required=False,
        default="No reason provided.",
    )
    parser.add_argument(
        "--celery_shutdown_timeout",
        help="Timeout in seconds for which to wait for Celery shutdown before terminating Celery processes individually."
        " Celery will wait for task completion after receiving a graceful shutdown, so this timeout should consider how "
        "long FireX shutdown should be willing to wait for remaining tasks.",
        default=DEFAULT_CELERY_SHUTDOWN_TIMEOUT,
        type=int,
    )
    args = parser.parse_args()
    logs_dir = args.logs_dir

    log_file = os.path.join(logs_dir, Uid.debug_dirname, "shutdown.log.txt")
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="[%(asctime)s %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return logs_dir, args.reason, args.celery_shutdown_timeout


def _shutdown_run(
    logs_dir: str,
    celery_shutdown_timeout: float,
    reason="No reason provided",
):
    logger.info(f"Shutting down due to reason: {reason}")
    logger.info(f"Shutting down with logs: {logs_dir}.")
    broker = BrokerFactory.broker_manager_from_logs_dir(logs_dir)
    celery_manager = CeleryManager(
        logs_dir=logs_dir,
        fx_env=FxEnvVars.load_firex_env_vars_from_env(),
    )
    celery_app = FireXCelery(
        fx_env=FxEnvVars.load_firex_env_vars_from_logs_dir(
            logs_dir, plugins="", broker_mgr=broker
        ),
        fx_expect_tasks=False,
        accept_content=["pickle", "json"],
    )
    try:
        if is_celery_responsive(broker, celery_app):
            revoke_active_tasks(
                broker,
                celery_app,
                cleanup_timeout=min(
                    celery_shutdown_timeout, _DEFAULT_REVOKE_CLEANUP_TIMEOUT
                ),
            )

            if is_celery_responsive(broker, celery_app):
                # double check celery responsiveness since control.shutdown()
                # can hang when broker is unresponsive, and broker might go
                # down between is_celery_responsive
                logger.info("Found active Celery; sending Celery shutdown.")
                celery_app.control.shutdown()
            else:
                logger.info("Celery appears unresponsive")

            celery_shutdown_success = celery_manager.wait_for_shutdown(
                celery_shutdown_timeout
            )
            if not celery_shutdown_success:
                logger.warning(
                    f"Celery not shutdown after {celery_shutdown_timeout} secs, force killing instead."
                )
                celery_manager.shutdown()
            else:
                logger.debug("Confirmed Celery shutdown successfully.")
        elif celery_manager.find_all_procs():
            logger.info(
                "Celery not active, but found celery processes to force shutdown."
            )
            celery_manager.shutdown()
        else:
            logger.info("No active Celery processes.")
    finally:
        if broker.is_alive():
            logger.info("Broker is alive; sending redis shutdown.")
            broker.shutdown()
            _wait_for_broker_shutdown(broker)
        else:
            logger.info("No active Broker.")

    logger.info(f"Shutdown of {logs_dir} complete.")


def main() -> None:
    logs_dir, reason, celery_shutdown_timeout = init()
    try:
        # Two launchers may both start before either subprocess takes ownership.
        lock = _run_shutdown_lock(
            logs_dir,
            # The log directory is on NFS. Leave time for the revoke wait,
            # graceful worker shutdown, and forced cleanup if needed.
            lifetime=2 * celery_shutdown_timeout + 10 * 60,
        )
        try:
            lock.lock(timeout=1)
        except TimeOutError:
            logger.info("Run shutdown is already in progress.")
            return
        try:
            _shutdown_run(logs_dir, celery_shutdown_timeout, reason)
        finally:
            lock.unlock()
    except BaseException:
        logger.exception("Shutdown failed.")
        raise
