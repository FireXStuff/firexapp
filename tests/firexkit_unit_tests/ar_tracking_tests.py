"""AsyncResult tracking: every live result must be disarmed before the broker goes.

celery's AsyncResult.__del__ calls into the backend unless ``backend`` is None, and
once the broker is down that call retries for as long as the retry policy allows. At
interpreter shutdown there is nothing left to interrupt it, so FireX tracks results
and clears their backends while the broker is still up.
"""

import gc
import weakref

import pytest
from celery.result import result_from_tuple

from firexkit.firex_celery import FireXCelery
from firexkit.result import FxAsyncResult


@pytest.fixture(autouse=True)
def ar_tracking():
    """Tracking is class state, so don't let it leak between tests."""
    original = FxAsyncResult._TRACKED_ARS
    FxAsyncResult._TRACKED_ARS = None
    FxAsyncResult.enable_ar_tracking()
    yield
    FxAsyncResult._TRACKED_ARS = original


def _deserialized_copy(result, app: FireXCelery):
    """Another object for the same task id, built the way celery builds them.

    This is the call behind AsyncResult._set_cache: every time task meta carrying
    children is deserialized, celery mints fresh result objects for ids it already
    has objects for.
    """
    return result_from_tuple(result.as_tuple(), app)


class TestEveryLiveResultIsDisarmed:
    def test_duplicates_of_one_task_id_are_all_disarmed(self, ut_app: FireXCelery):
        """The registry used to be keyed by task id, so only the last one was cleared.

        Keying by task id is not enough, and neither is a set: AsyncResult hashes and
        compares by task id, so a set collapses these three into one as well.
        """
        original = ut_app.AsyncResult("task-1")
        copies = [_deserialized_copy(original, ut_app) for _ in range(2)]
        all_results = [original, *copies]
        assert len({id(r) for r in all_results}) == 3, "precondition: distinct objects"
        assert len({r.id for r in all_results}) == 1, "precondition: one task id"

        FxAsyncResult.disable_all_ar_backends()

        assert [r.backend for r in all_results] == [None, None, None]

    def test_a_parent_built_during_deserialization_is_disarmed(
        self, ut_app: FireXCelery
    ):
        """result_from_tuple rebuilds parents recursively; those count too.

        Both parent objects have to end up disarmed. Keying by task id disarms only
        the rebuilt one, because it was constructed last and displaced the original.
        """
        parent = ut_app.AsyncResult("parent-1")
        child = ut_app.AsyncResult("child-1", parent=parent)
        rebuilt = _deserialized_copy(child, ut_app)
        assert rebuilt.parent is not parent, "precondition: parent was rebuilt"

        FxAsyncResult.disable_all_ar_backends()

        assert rebuilt.parent.backend is None
        assert parent.backend is None, "the displaced parent was left armed"

    def test_a_forgotten_result_is_still_disarmed(self, ut_app: FireXCelery):
        """Forgetting drops the result's cache, not the object: its __del__ still runs."""
        result = ut_app.AsyncResult("task-1")
        result.fx_forget()

        FxAsyncResult.disable_all_ar_backends()

        assert result.backend is None


class TestTrackingDoesNotPinResults:
    def test_a_dropped_result_leaves_the_registry(self, ut_app: FireXCelery):
        """Tracking holds strong references no longer: a run makes a great many
        results, and keeping every one alive until exit both grows the heap and
        guarantees a pile of __del__ calls during interpreter shutdown."""
        result = ut_app.AsyncResult("task-1")
        observer = weakref.ref(result)
        assert len(FxAsyncResult._TRACKED_ARS) == 1

        del result
        gc.collect()

        assert observer() is None, "the registry is still holding the result"
        assert len(FxAsyncResult._TRACKED_ARS) == 0


class TestTrackingIsOptional:
    def test_results_are_untracked_until_tracking_is_enabled(self, ut_app: FireXCelery):
        FxAsyncResult._TRACKED_ARS = None
        result = ut_app.AsyncResult("task-1")

        FxAsyncResult.disable_all_ar_backends()

        assert result.backend is not None, "untracked results are left alone"
