"""How CeleryManager launches a celery worker, and when it refuses to.

The worker is backgrounded by a shell that is gone as soon as it has forked, so
from here it is observable only through the files it was told to write and the
process table -- which is also all CeleryManager has. These drive
start_celery_worker against a stand-in celery that records its argv and then
behaves the way a real worker would.
"""

import json
import os
import signal
import sys
import time

import psutil
import pytest

import firexapp.celery_manager
from firexapp.celery_manager import (
    CeleryManager,
    CeleryVenvMismatchError,
    CeleryWorkerStartFailed,
)
from firexapp.common import FIREX_BIN_DIR_ENV
from firexapp.engine.default_celery_config import FxEnvVars

# The install a cascading invocation leaks down to its child.
LEAKED_BIN_DIR = "/auto/firex/STAGINGS/MASTER_260930-133104/firex/venv/bin"

# Long enough that a worker failing to come up is a test that hangs rather than
# a test that passes for the wrong reason, short enough to notice.
UT_TIMEOUT = 60

UT_QUEUE = "ut_queue"


@pytest.fixture
def ut_venv(tmp_path, monkeypatch):
    """The venv the process under test is running out of."""
    venv = tmp_path / "firex" / "venv"
    (venv / "bin").mkdir(parents=True)
    monkeypatch.setenv("VIRTUAL_ENV", str(venv))
    monkeypatch.delenv(FIREX_BIN_DIR_ENV, raising=False)
    return venv


def _ut_celery_manager(logs_dir, env=None) -> CeleryManager:
    return CeleryManager(
        logs_dir=str(logs_dir),
        fx_env=FxEnvVars(
            CURRENT_RUN_FIREX_ID="FireX-ut-260101-000000-1",
            firex_logs_dir=str(logs_dir),
            redis_bin_dir="/usr/bin",
            BROKER="redis://localhost:6379/0",
        ),
        env=env,
    )


# firex_bin_dir and VIRTUAL_ENV must describe the same install.
#
# qualify_firex_bin resolves celery to an absolute path under firex_bin_dir, so
# that variable alone picks the interpreter the worker runs, while the worker's
# inherited PYTHONPATH still resolves modules out of VIRTUAL_ENV. firex_bin_dir
# is inherited, so a cascading invocation -- a run that builds a new install and
# then submits into it -- can hand its own value down to a child running out of
# a different venv.


def test_worker_refused_when_bin_dir_and_venv_disagree(ut_venv, tmp_path):
    celery_manager = _ut_celery_manager(
        tmp_path, env={FIREX_BIN_DIR_ENV: LEAKED_BIN_DIR}
    )

    with pytest.raises(CeleryVenvMismatchError) as exc_info:
        celery_manager.start_celery_worker(workername="ut_worker", queues=UT_QUEUE)

    # Both installs are named, so the run that mixed them is identifiable.
    message = str(exc_info.value)
    assert LEAKED_BIN_DIR in message
    assert str(ut_venv) in message


def test_worker_allowed_when_bin_dir_matches_venv(ut_venv, tmp_path, monkeypatch):
    monkeypatch.setenv(FIREX_BIN_DIR_ENV, str(ut_venv / "bin"))

    _ut_celery_manager(tmp_path)._assert_worker_venv_consistent()


def test_trailing_separators_are_not_a_mismatch(ut_venv, tmp_path, monkeypatch):
    monkeypatch.setenv("VIRTUAL_ENV", f"{ut_venv}/")
    monkeypatch.setenv(FIREX_BIN_DIR_ENV, f"{ut_venv}//bin")

    _ut_celery_manager(tmp_path)._assert_worker_venv_consistent()


def test_no_bin_dir_leaves_celery_to_the_path(ut_venv, tmp_path):
    # Without firex_bin_dir there is no second install to disagree with: celery
    # comes off PATH, which venv activation keeps in step with VIRTUAL_ENV.
    assert FIREX_BIN_DIR_ENV not in os.environ

    _ut_celery_manager(tmp_path)._assert_worker_venv_consistent()


_FAKE_CELERY_SCRIPT = '''\
"""A stand-in for the celery worker that records how it was invoked.

Imitates what CeleryManager is left to cope with: the shell that backgrounded
this is gone as soon as it has forked, so the pid file is the only sign of a
worker that got as far as booting, and whatever it printed on the way is the
only sign of one that didn't.
"""
import json
import os
import sys
import time


def _opt(name):
    values = [a.split("=", 1)[1] for a in argv if a.startswith(f"--{name}=")]
    assert len(values) == 1, f"expected exactly one --{name} in {argv}"
    return values[0]


argv = sys.argv[1:]
behaviour = os.environ.get("UT_CELERY_BEHAVIOUR", "boots")

with open(os.environ["UT_CELERY_ARGV_FILE"], "w") as argv_file:
    json.dump(argv, argv_file)

if behaviour == "fails_before_logging":
    # An --app that won't import. Celery hasn't opened its log file yet, so it
    # reports on the stderr it inherited from the shell.
    sys.stderr.write("ModuleNotFoundError: No module named 'nosuchapp'\\n")
    sys.exit(2)

time.sleep(float(os.environ.get("UT_CELERY_BOOT_SECS", "0")))

if behaviour == "dies_while_booting":
    # Once logging is up celery reports through its own log file instead.
    with open(_opt("logfile"), "a") as logfile:
        logfile.write("ConnectionError: broker refused the connection\\n")
    os._exit(1)

with open(_opt("pidfile"), "w") as pid_file:
    pid_file.write(str(os.getpid()))

time.sleep(float(os.environ.get("UT_CELERY_LIFETIME_SECS", "10")))
'''


class _FakeCelery:
    """The stand-in celery, and the argv it last recorded."""

    def __init__(self, path, argv_file):
        self.path = path
        self._argv_file = argv_file

    @property
    def argv(self) -> list[str]:
        return json.loads(self._argv_file.read_text())

    def opt(self, name: str) -> str:
        """The single value celery received for '--name'.

        A path that a shell had split on a space arrives as two arguments, so
        asking for one value is what makes that visible.
        """
        values = [
            arg.split("=", 1)[1] for arg in self.argv if arg.startswith(f"--{name}=")
        ]
        assert len(values) == 1, f"expected exactly one --{name} in {self.argv}"
        return values[0]


@pytest.fixture
def fake_celery(ut_venv, tmp_path, monkeypatch):
    celery_path = ut_venv / "bin" / "celery"
    celery_path.write_text(f"#!{sys.executable}\n{_FAKE_CELERY_SCRIPT}")
    celery_path.chmod(0o755)

    # qualify_firex_bin reads the process environment rather than the manager's,
    # and CeleryManager snapshots os.environ, so both have to be set up here.
    monkeypatch.setenv(FIREX_BIN_DIR_ENV, str(ut_venv / "bin"))
    monkeypatch.setenv("UT_CELERY_ARGV_FILE", str(tmp_path / "celery_argv.json"))

    yield _FakeCelery(path=celery_path, argv_file=tmp_path / "celery_argv.json")

    # A backgrounded stand-in outlives the shell that started it by design;
    # don't let one outlive the test as well.
    for proc in psutil.process_iter(["cmdline"]):
        try:
            if str(celery_path) in (proc.info["cmdline"] or []):
                proc.kill()
        except psutil.Error:
            pass


def test_paths_reach_celery_whole_when_the_logs_dir_has_a_space(fake_celery, tmp_path):
    """Every path celery is given is built from the caller's logs dir.

    The command is built as a list and joined with shlex.join for this reason:
    the worker is backgrounded by a shell, and a shell splits an unquoted log
    file or pid file on whitespace. The first sign of that would have been a
    worker that never writes a pid file.
    """
    logs_dir = tmp_path / "logs dir with spaces"

    _ut_celery_manager(logs_dir).start_celery_worker(
        workername="ut_worker",
        queues=UT_QUEUE,
        timeout=UT_TIMEOUT,
    )

    assert fake_celery.opt("logfile").startswith(f"{logs_dir}/")
    assert fake_celery.opt("pidfile").startswith(f"{logs_dir}/")


def test_the_launch_returns_while_the_worker_is_still_running(
    fake_celery, tmp_path, monkeypatch
):
    """The worker outlives the call that started it.

    A submit that isn't waiting for its run (--sync false) returns once the
    worker is up, so a launch that waited on celery would hold every such
    submit open for the whole length of the run.
    """
    worker_lifetime_secs = 30
    monkeypatch.setenv("UT_CELERY_LIFETIME_SECS", str(worker_lifetime_secs))
    celery_manager = _ut_celery_manager(tmp_path)

    start_time = time.monotonic()
    worker_id = celery_manager.start_celery_worker(
        workername="ut_worker",
        queues=UT_QUEUE,
        timeout=UT_TIMEOUT,
    )

    assert time.monotonic() - start_time < worker_lifetime_secs / 2
    assert psutil.pid_exists(CeleryManager.get_celery_pid(str(tmp_path), worker_id))


def test_failure_before_logging_is_reported_without_waiting_out_the_timeout(
    fake_celery, tmp_path, monkeypatch
):
    """A worker that fails before it has a log file still reports on stderr.

    The shell that backgrounded it reports its own exit status, which is 0
    however celery fared, so what is left to go on is a worker that is gone and
    the stdout file it inherited.
    """
    monkeypatch.setenv("UT_CELERY_BEHAVIOUR", "fails_before_logging")
    monkeypatch.setattr(firexapp.celery_manager, "_LIVENESS_POLL_INTERVAL_SECS", 0.2)

    start_time = time.monotonic()
    with pytest.raises(CeleryWorkerStartFailed) as exc_info:
        _ut_celery_manager(tmp_path).start_celery_worker(
            workername="ut_worker",
            queues=UT_QUEUE,
            timeout=UT_TIMEOUT,
        )

    assert time.monotonic() - start_time < UT_TIMEOUT / 2
    # What the worker printed, not just the name of the file it printed it to.
    assert "No module named 'nosuchapp'" in str(exc_info.value)


def test_worker_dying_while_booting_is_not_waited_out(
    fake_celery, tmp_path, monkeypatch
):
    """Once celery has logging up, a dead worker and a slow one look alike from
    out here: a pid file that isn't there. Only noticing the process has gone
    tells them apart."""
    monkeypatch.setenv("UT_CELERY_BEHAVIOUR", "dies_while_booting")
    monkeypatch.setattr(firexapp.celery_manager, "_LIVENESS_POLL_INTERVAL_SECS", 0.2)

    start_time = time.monotonic()
    with pytest.raises(CeleryWorkerStartFailed) as exc_info:
        _ut_celery_manager(tmp_path).start_celery_worker(
            workername="ut_worker",
            queues=UT_QUEUE,
            timeout=UT_TIMEOUT,
        )

    assert time.monotonic() - start_time < UT_TIMEOUT / 2
    message = str(exc_info.value)
    assert "exited before it became active" in message
    # The worker log file is the only thing a post-fork failure can report
    # through, so it has to be read as well as named.
    assert "broker refused the connection" in message


def test_slow_worker_is_not_mistaken_for_a_dead_one(fake_celery, tmp_path, monkeypatch):
    """A worker still booting is not a worker that has died.

    The pid file is absent either way, so the liveness check is what separates
    them -- and it has to find a worker that the shell backgrounded, which is
    no longer a child of anything this process can wait on.
    """
    monkeypatch.setenv("UT_CELERY_BOOT_SECS", "1.5")
    monkeypatch.setattr(firexapp.celery_manager, "_LIVENESS_POLL_INTERVAL_SECS", 0.2)
    celery_manager = _ut_celery_manager(tmp_path)

    worker_id = celery_manager.start_celery_worker(
        workername="ut_worker",
        queues=UT_QUEUE,
        timeout=UT_TIMEOUT,
    )

    assert psutil.pid_exists(CeleryManager.get_celery_pid(str(tmp_path), worker_id))


def test_shutdown_wait_watches_the_processes_not_the_pid_file(fake_celery, tmp_path):
    """A worker killed outright leaves its pid file behind.

    Celery only unlinks that file from an atexit handler
    (celery.platforms.create_pidlock), so a wait on the file alone would never
    return for a worker the OOM killer took -- and that wait is what holds a
    containerised worker's container open.
    """
    celery_manager = _ut_celery_manager(tmp_path)
    worker_id = celery_manager.start_celery_worker(
        workername="ut_worker",
        queues=UT_QUEUE,
        timeout=UT_TIMEOUT,
    )
    pid_file = celery_manager.pid_files[worker_id]

    assert not celery_manager.wait_for_worker_shutdown(worker_id, timeout=0.5), (
        "a running worker is not shut down"
    )

    os.kill(CeleryManager.get_celery_pid(str(tmp_path), worker_id), signal.SIGKILL)

    assert celery_manager.wait_for_worker_shutdown(worker_id, timeout=UT_TIMEOUT)
    assert os.path.isfile(pid_file), (
        "the pid file outlives the worker, which is the point of not watching it"
    )
