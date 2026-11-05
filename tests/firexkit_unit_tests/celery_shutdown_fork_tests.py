"""A reply-enabled FireX control call must not poison subsequent forks.

Observed after the Celery 5.6.3 / Kombu 5.6.2 upgrade: shutdown(reply=True)
can leave a pending Redis BRPOP on a pooled connection. The first child's
after-fork cleanup consumes the BRPOP response from the inherited socket,
but the parent's poll state still says a read is pending. The next child
can then block reading that socket during Kombu cleanup, before its target
runs. In CI this appeared as intermittent plugin-metadata extraction
timeouts, rather than a failure inside the plugin itself.

Before the upgrade, Kombu 5.3.1's Redis Channel.close() disconnected without
first reading a pending BRPOP response. Kombu 5.6.2 calls _brpop_read() when
_in_poll is set, including during after-fork pool cleanup. That added read
exposes the stale poll state on the shared socket; this regression passes
with the pre-upgrade Celery 5.3.1 / Kombu 5.3.1 dependencies.

Use a real, private Redis broker: a mocked control API cannot reproduce
Kombu reading an inherited socket during multiprocessing's after-fork cleanup.
Run the scenario in a fresh interpreter so other unit tests' apps, pools and
after-fork callbacks cannot affect it.
"""

import multiprocessing
import os
import shutil
import signal
import socket
import subprocess
import time
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import redis

_BROKER_START_TIMEOUT = 5
_CHILD_TIMEOUT = 5
_PROBE_TIMEOUT = 30
_CLEANUP_TIMEOUT = 5


@contextmanager
def _private_redis_broker(logs_dir):
    redis_server = shutil.which("redis-server")
    if redis_server is None:
        raise RuntimeError("This regression requires redis-server on PATH")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]

    with (logs_dir / "redis.log").open("w") as log:
        server = subprocess.Popen(
            [
                redis_server,
                "--bind",
                "127.0.0.1",
                "--port",
                str(port),
                "--protected-mode",
                "yes",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                str(logs_dir),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        client = redis.Redis(
            host="127.0.0.1", port=port, socket_connect_timeout=0.2, socket_timeout=0.2
        )
        try:
            deadline = time.monotonic() + _BROKER_START_TIMEOUT
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError((logs_dir / "redis.log").read_text())
                try:
                    if client.ping():
                        break
                except (
                    redis.exceptions.ConnectionError,
                    redis.exceptions.TimeoutError,
                ):
                    time.sleep(0.02)
            else:
                raise TimeoutError("Private Redis broker did not become ready")

            # Do not send a control command if another server won the port race.
            if client.info("server")["process_id"] != server.pid:
                raise RuntimeError("The Redis server is not this test's private broker")
            yield f"redis://127.0.0.1:{port}/0"
        finally:
            client.close()
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=_CLEANUP_TIMEOUT)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=_CLEANUP_TIMEOUT)


def _fork_results_after_shutdown(broker_url, logs_dir):
    from kombu import Connection

    from firexapp.engine.default_celery_config import FxEnvVars
    from firexkit.testing import ut_celery_app

    fx_env = FxEnvVars.create_no_task_exec_fx_env(logs_dir=str(logs_dir))
    fx_env.BROKER = broker_url
    app = ut_celery_app(
        "shutdown-fork-regression",
        fx_env=fx_env,
        fx_expect_tasks=False,
    )
    # Keep the reply consumer's BRPOP outstanding when its shorter RPC wait ends.
    # This is the real control implementation, not the in-memory worker fake.
    app.conf.broker_transport_options = {"polling_interval": 2}
    results = []
    try:
        if app.conf.broker_write_url != broker_url:
            raise RuntimeError("The probe must only use its private Redis broker")
        # No workers connect to this private broker: exercise the real control
        # reply path without shutting down any running worker.
        app.control.shutdown(
            destination=["never-started-regression-worker"],
            reply=True,
            timeout=0.1,
        )
        ctx = multiprocessing.get_context("fork")
        # The first child can drain a reply using its copied channel state.
        # The second inherits the parent's stale state but no reply to read.
        for _ in range(2):
            reached_target = ctx.Event()
            child = ctx.Process(target=reached_target.set)
            try:
                child.start()
                child.join(timeout=_CHILD_TIMEOUT)
            finally:
                if child.is_alive():
                    child.kill()
                    child.join(timeout=_CLEANUP_TIMEOUT)
            results.append(
                {"reached_target": reached_target.is_set(), "exitcode": child.exitcode}
            )
            child.close()
        return results
    finally:
        # The first fork can consume the pending reply without updating the
        # parent's copied poll state. Disconnect sockets before collecting the
        # private pool, or regression-test teardown could repeat the same hang.
        pool = app.pool
        for connection in list(pool._resource.queue) + list(pool._dirty):
            if (
                isinstance(connection, Connection)
                and connection._default_channel is not None
            ):
                connection._default_channel._disconnect_pools()
        pool.force_close_all()
        app.close()


def _shutdown_then_fork(broker_url, logs_dir, result_writer):
    # The supervising test can kill this process group if the scenario hangs.
    os.setsid()
    with result_writer:
        result_writer.send(_fork_results_after_shutdown(broker_url, logs_dir))


def _run_fork_probe(broker_url, logs_dir):
    # Spawn, rather than fork, so the probe inherits no apps or connection pools
    # from other unit tests. Only its two deliberately tested children use fork.
    ctx = multiprocessing.get_context("spawn")
    reader, writer = ctx.Pipe(duplex=False)
    probe = ctx.Process(target=_shutdown_then_fork, args=(broker_url, logs_dir, writer))
    try:
        probe.start()
        writer.close()
        if not reader.poll(_PROBE_TIMEOUT):
            raise AssertionError(
                "Shutdown/fork probe did not report within its deadline"
            )
        try:
            results = reader.recv()
        except EOFError as exc:
            probe.join(timeout=_CLEANUP_TIMEOUT)
            raise AssertionError(
                f"Shutdown/fork probe exited without results: exitcode={probe.exitcode}"
            ) from exc
        probe.join(timeout=_CLEANUP_TIMEOUT)
        if probe.is_alive() or probe.exitcode != 0:
            raise AssertionError(
                f"Shutdown/fork probe did not exit cleanly: exitcode={probe.exitcode}"
            )
        return results
    finally:
        reader.close()
        writer.close()
        if probe.pid is not None:
            if probe.is_alive():
                try:
                    os.killpg(probe.pid, signal.SIGKILL)
                except ProcessLookupError:
                    # Spawn may fail before the target creates its process group.
                    probe.kill()
            probe.join(timeout=_CLEANUP_TIMEOUT)
        probe.close()


@unittest.skipUnless(
    "fork" in multiprocessing.get_all_start_methods(),
    "Requires the multiprocessing fork start method",
)
class CeleryShutdownForkTests(unittest.TestCase):
    def setUp(self):
        resources = ExitStack()
        self.addCleanup(resources.close)
        self.logs_dir = Path(resources.enter_context(TemporaryDirectory()))
        self.broker_url = resources.enter_context(_private_redis_broker(self.logs_dir))

    def test_reply_enabled_shutdown_allows_subsequent_forks(self):
        results = _run_fork_probe(self.broker_url, self.logs_dir)
        self.assertEqual(len(results), 2)
        for number, result in enumerate(results, 1):
            with self.subTest(fork=number):
                self.assertEqual(
                    result,
                    {"reached_target": True, "exitcode": 0},
                    f"Fork {number} after FireXCelery.control.shutdown did not complete. "
                    "Kombu after-fork cleanup must not block on an inherited Redis reply connection.",
                )
