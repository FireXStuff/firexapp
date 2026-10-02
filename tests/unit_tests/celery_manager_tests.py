"""firex_bin_dir and VIRTUAL_ENV must describe the same install.

qualify_firex_bin resolves celery to an absolute path under firex_bin_dir, so
that variable alone picks the interpreter the worker runs, while the worker's
inherited PYTHONPATH still resolves modules out of VIRTUAL_ENV. firex_bin_dir is
inherited, so a cascading invocation -- a run that builds a new install and then
submits into it -- can hand its own value down to a child running out of a
different venv.
"""

import os

import pytest

from firexapp.celery_manager import CeleryManager, CeleryVenvMismatchError
from firexapp.common import FIREX_BIN_DIR_ENV
from firexapp.engine.default_celery_config import FxEnvVars

# The install a cascading invocation leaks down to its child.
LEAKED_BIN_DIR = "/auto/firex/STAGINGS/MASTER_260930-133104/firex/venv/bin"


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


def test_worker_refused_when_bin_dir_and_venv_disagree(ut_venv, tmp_path):
    celery_manager = _ut_celery_manager(
        tmp_path, env={FIREX_BIN_DIR_ENV: LEAKED_BIN_DIR}
    )

    with pytest.raises(CeleryVenvMismatchError) as exc_info:
        celery_manager.start_celery_worker(workername="ut_worker")

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
