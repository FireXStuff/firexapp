"""Celery 5.6 logs a warning when the host timezone files disagree.

enable_utc is off, so Celery asks tzlocal and, on failure, uses LocalTimezone.
That object is what Celery 5.3.1 used. The filter drops the warning and leaves
the fallback in place.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from firexkit.firex_celery import silence_local_timezone_warning

_SRC = str(Path(__file__).resolve().parents[2])


def test_filter_drops_only_the_local_timezone_warning():
    silence_local_timezone_warning()
    tz_logger = logging.getLogger("celery.utils.time")
    seen: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            seen.append(record)

    handler = _Capture(level=logging.DEBUG)
    previous_level = tz_logger.level
    tz_logger.addHandler(handler)
    tz_logger.setLevel(logging.DEBUG)
    try:
        tz_logger.warning(
            "Failed to retrieve local timezone (%s): %s",
            "ZoneInfoNotFoundError",
            "conflict",
        )
        tz_logger.warning("broker connection lost")
    finally:
        tz_logger.removeHandler(handler)
        tz_logger.setLevel(previous_level)

    assert [record.getMessage() for record in seen] == ["broker connection lost"]


def _read_local_timezone(install_filter: bool) -> tuple[str, str]:
    """Kind of celery.utils.time.timezone.local, and the warnings it logged.

    A fresh interpreter: the lookup is cached on the process, and this one may
    already have imported Celery.
    """
    script = """
import logging
import os
import sys

logging.basicConfig(level=logging.WARNING, stream=sys.stderr, format="%(message)s")
os.environ.pop("TZ", None)
if install_filter:
    import firexkit.firex_celery
from celery.utils.time import timezone

sys.stdout.write(type(timezone.local).__name__)
"""
    env = os.environ.copy()
    env.pop("TZ", None)
    env["PYTHONPATH"] = _SRC + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", script.replace("install_filter", "True" if install_filter else "False")],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout, completed.stderr


def test_without_the_filter_a_conflicting_host_warns_and_uses_local_timezone():
    kind, stderr = _read_local_timezone(install_filter=False)
    if "Failed to retrieve local timezone" not in stderr:
        pytest.skip("this host's timezone files agree")
    assert kind == "LocalTimezone"


def test_importing_the_worker_app_silences_the_warning_and_keeps_local_timezone():
    kind, stderr = _read_local_timezone(install_filter=True)
    assert kind == "LocalTimezone"
    assert "Failed to retrieve local timezone" not in stderr
