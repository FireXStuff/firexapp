#
# Fixtures shared by all of firexapp's unit test trees.
#
import pytest

from firexkit.testing import ut_isolate_shared_tasks

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
    # e.g. chain_tests' test_bad_returns_code deliberately defines tasks that raise
    # when created, which would otherwise break whichever unrelated test next
    # finalizes an app, such as plugins_tests' test_plugin_env.
    with ut_isolate_shared_tasks():
        yield
