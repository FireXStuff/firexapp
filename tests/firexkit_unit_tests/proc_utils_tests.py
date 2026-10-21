"""kill_procs_by_name_and_cmdline, against real processes."""

import signal
import subprocess
import sys
import time
import uuid

import psutil
import pytest

from firexkit.proc_utils import kill_procs_by_name_and_cmdline


@pytest.fixture
def python(tmp_path) -> str:
    """An interpreter whose processes are named 'python', whatever this one is called."""
    exe = tmp_path / "python"
    exe.symlink_to(sys.executable)
    return str(exe)


def _zombie(python: str) -> subprocess.Popen:
    proc = subprocess.Popen([python, "-c", "pass"])
    # it's a zombie once it has exited, until it's waited on.
    deadline = time.monotonic() + 10
    while psutil.Process(proc.pid).status() != psutil.STATUS_ZOMBIE:
        assert time.monotonic() < deadline, "it never exited"
        time.sleep(0.01)
    return proc


def test_a_zombie_by_the_same_name_doesnt_stop_the_search(python):
    # Matches nothing else that might be running here.
    marker = f"fx-proc-utils-test-{uuid.uuid4()}"
    zombie = _zombie(python)
    target = subprocess.Popen([python, "-c", "import time; time.sleep(60)", marker])
    try:
        killed = kill_procs_by_name_and_cmdline("python", marker)

        assert [proc.pid for proc in killed] == [target.pid]
        assert target.wait(timeout=10) == -signal.SIGKILL
    finally:
        target.kill()
        target.wait()
        zombie.wait()
