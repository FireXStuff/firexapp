"""SubmitBaseApp.self_destruct must disarm AsyncResults before the broker goes down.

Any AsyncResult that still holds a backend talks to it from __del__, and once the
broker is gone that call retries for as long as the retry policy allows. During
interpreter shutdown there is nothing left to interrupt it.
"""

from unittest import mock

import pytest

from firexapp.submit.submit import SubmitBaseApp
from firexkit.result import FxAsyncResult
from firexkit.testing import ut_backed_celery_app


@pytest.fixture(autouse=True)
def ar_tracking():
    """Tracking is class state, so don't let it leak between tests."""
    original = FxAsyncResult._TRACKED_ARS
    FxAsyncResult._TRACKED_ARS = None
    FxAsyncResult.enable_ar_tracking()
    yield
    FxAsyncResult._TRACKED_ARS = original


@pytest.fixture
def tracked_result():
    """One live AsyncResult, as a run leaves behind by the thousand."""
    return ut_backed_celery_app().AsyncResult("task-1")


@pytest.fixture
def app_with_broker(tmp_path) -> SubmitBaseApp:
    app = SubmitBaseApp()
    # Only truthiness matters here: self_destruct does broker cleanup when set.
    app.broker = mock.Mock()
    # A real logs dir, so the run-complete file self_destruct writes goes somewhere
    # harmless instead of needing its own mock.
    app.uid = mock.Mock(logs_dir=str(tmp_path))
    return app


class TestDisarmingResultsBeforeShutdown:
    def test_results_are_disarmed_without_a_root_result(
        self, app_with_broker, tracked_result
    ):
        """Disarming used to be nested under `if root_async_result:`, but the
        shutdown below it is unconditional -- so a run that never produced a root
        result lost its broker with every result still armed."""
        with mock.patch("firexapp.submit.submit.launch_background_shutdown"):
            app_with_broker.self_destruct(chain_details=None)

        assert tracked_result.backend is None

    def test_results_are_disarmed_before_shutdown_is_launched(
        self, app_with_broker, tracked_result
    ):
        """Ordering is the whole point: disarming after the broker is gone is useless."""
        backend_when_shutdown_launched = []

        def record_shutdown(*args, **kwargs):
            backend_when_shutdown_launched.append(tracked_result.backend)

        with mock.patch(
            "firexapp.submit.submit.launch_background_shutdown",
            side_effect=record_shutdown,
        ):
            app_with_broker.self_destruct(chain_details=None)

        assert backend_when_shutdown_launched == [None]

    def test_nothing_is_disarmed_when_there_is_no_broker(self, tracked_result):
        """No broker means no shutdown, so the results stay usable."""
        app = SubmitBaseApp()
        assert app.broker is None

        app.self_destruct(chain_details=None)

        assert tracked_result.backend is not None
