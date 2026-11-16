#
# Fixtures shared by all of firexapp's unit test trees.
#
import celery._state
import pytest

# integration_tests are flow_tests configurations (see firex_bundle_ci), not pytest
# tests. They also reuse unit_tests module basenames, which pytest can't import side
# by side, and register tasks on the shared firexapp.engine.celery.app when imported.
collect_ignore = ["integration_tests"]


def pytest_ignore_collect(collection_path, config):
    # The workspace pytest.ini's python_files (*test*.py) matches conftest.py too, so
    # without this each one is also imported as a test module named 'conftest', and
    # all but one fail on the basename clash. pytest loads them as conftests regardless.
    if collection_path.name == "conftest.py":
        return True
    return None


@pytest.fixture(autouse=True)
def _ut_isolate_shared_tasks():
    """
    Stop the tasks a test defines from being recreated on every app finalized after it.

    @app.task defaults to shared=True, which registers a process-wide callback that
    recreates the task on each app finalized later, e.g. firexapp.engine.celery.app
    the first time its tasks are read. A task a test deliberately defines badly, as
    chain_tests' test_bad_returns_code does, then raises in whichever unrelated test
    next finalizes an app.
    """
    saved = set(celery._state._on_app_finalizers)
    yield
    celery._state._on_app_finalizers.clear()
    celery._state._on_app_finalizers.update(saved)
