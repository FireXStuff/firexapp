"""launch_background_shutdown must ride out fork() briefly failing for lack of memory.

A run that ends while its host is short of memory is the run most likely to have its
shutdown fork fail, and a shutdown that never launches leaks the run's broker, celery
workers and tracking services.
"""

import errno
import logging
import os

import psutil
import pytest

from firexapp.submit import shutdown
from firexapp.submit.shutdown import launch_background_shutdown


class _FlakyFork:
    """os.fork, but its first `failures` calls fail the way fork() does on a host
    that is out of memory."""

    def __init__(self, failures: int, err: int):
        self.failures = failures
        self.err = err
        self.calls = 0
        self._real_fork = os.fork

    def __call__(self) -> int:
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError(self.err, os.strerror(self.err))
        return self._real_fork()


@pytest.fixture
def flaky_fork(monkeypatch):
    def _install(failures: int, err: int = errno.ENOMEM) -> _FlakyFork:
        fork = _FlakyFork(failures, err)
        monkeypatch.setattr(os, "fork", fork)
        return fork

    return _install


@pytest.fixture(autouse=True)
def no_retry_delays(monkeypatch):
    """The real delays add up to half a minute; what they space out is the same."""
    monkeypatch.setattr(
        shutdown,
        "_SHUTDOWN_LAUNCH_RETRY_DELAYS",
        (0,) * len(shutdown._SHUTDOWN_LAUNCH_RETRY_DELAYS),
    )


@pytest.fixture
def fake_firex_shutdown(tmp_path, monkeypatch):
    """Launched exactly as firex_shutdown is, but does nothing until it's killed."""
    script = tmp_path / "firex_shutdown"
    script.write_text("#!/bin/sh\nexec sleep 60\n")
    script.chmod(0o755)
    monkeypatch.setattr(shutdown, "qualify_firex_bin", lambda name: str(script))

    launched = []
    yield launched
    for pid in launched:
        try:
            psutil.Process(pid).kill()
        except psutil.NoSuchProcess:
            pass


@pytest.mark.parametrize("err", [errno.ENOMEM, errno.EAGAIN])
def test_shutdown_launches_once_fork_stops_failing(
    tmp_path, flaky_fork, fake_firex_shutdown, caplog, err
):
    fork = flaky_fork(failures=2, err=err)

    with caplog.at_level(logging.WARNING, logger=shutdown.__name__):
        pid = launch_background_shutdown(str(tmp_path), reason=None)
    fake_firex_shutdown.append(pid)

    assert fork.calls == 3
    assert psutil.Process(pid).is_running()
    assert caplog.text.count("retrying") == 2
    assert "FAILED TO LAUNCH" not in caplog.text


def test_shutdown_launch_gives_up_if_fork_keeps_failing(
    tmp_path, flaky_fork, fake_firex_shutdown, caplog
):
    fork = flaky_fork(failures=1000)

    with (
        caplog.at_level(logging.WARNING, logger=shutdown.__name__),
        pytest.raises(OSError) as e,
    ):
        launch_background_shutdown(str(tmp_path), reason=None)

    assert e.value.errno == errno.ENOMEM
    assert fork.calls == len(shutdown._SHUTDOWN_LAUNCH_RETRY_DELAYS) + 1
    assert "SHUTDOWN PROCESS FAILED TO LAUNCH" in caplog.text


def test_shutdown_launch_does_not_retry_errors_that_wont_go_away(
    tmp_path, flaky_fork, fake_firex_shutdown
):
    fork = flaky_fork(failures=1, err=errno.EPERM)

    with pytest.raises(PermissionError):
        launch_background_shutdown(str(tmp_path), reason=None)

    assert fork.calls == 1
