"""
    Tasks declaring @app.task(forget=True) are forgotten by whoever enqueues them.
"""
from functools import partial
from unittest import mock
from uuid import uuid4

import pytest

from firexkit.chain import SignatureX
from firexkit.firex_celery import FireXCelery
from firexkit.result import FxAsyncResult, ManyFxAsyncResults
from firexkit.task import FireXTask


@pytest.fixture
def remembered(ut_app: FireXCelery):
    @ut_app.task(base=FireXTask)
    def remembered():
        pass

    return remembered


@pytest.fixture
def forgotten(ut_app: FireXCelery):
    @ut_app.task(base=FireXTask, forget=True)
    def forgotten():
        pass

    return forgotten


class TestDeclaringForget:

    def test_a_task_is_remembered_unless_it_says_otherwise(self, remembered):
        assert remembered.forget is False
        assert remembered.s().declares_forget() is False

    def test_a_task_can_declare_itself_forgettable(self, forgotten):
        assert forgotten.forget is True
        assert forgotten.s().declares_forget() is True

    def test_a_chain_is_forgettable_when_any_of_its_tasks_is(self, remembered, forgotten):
        # the whole chain's results are forgotten together, so one task
        # asking for it is enough.
        assert (remembered.s() | forgotten.s()).declares_forget() is True
        assert (forgotten.s() | remembered.s()).declares_forget() is True

    def test_a_chain_of_remembered_tasks_is_not_forgettable(self, remembered):
        assert (remembered.s() | remembered.s()).declares_forget() is False


def _enqueue_child(child_result, declares_forget: bool, **kwargs) -> mock.Mock:
    """Enqueue a chain that declares (or doesn't) forget, and report the enqueuing task."""
    chain = mock.Mock(spec=SignatureX)
    chain.declares_forget.return_value = declares_forget
    chain.apply_async_x.return_value = child_result

    task = mock.Mock(spec=FireXTask)
    task._resolve_queue.return_value = None
    task.context = mock.Mock() # set by FireXTask.__init__, so not part of the spec.

    FireXTask.enqueue_child(task, chain, **kwargs)
    return task


def _is_flagged_to_forget(child_result: FxAsyncResult) -> bool:
    """Whether the child was told to forget itself, read back the way postrun does."""
    return bool(
        child_result.app.backend_hget_task_attr(child_result.id, '_fx_forget')
    )


class TestEnqueueChildForget:
    """A non-blocking enqueue only forgets when it was explicitly asked to."""

    @pytest.fixture
    def child_result(self, ut_app: FireXCelery) -> FxAsyncResult:
        # a real result over the in-memory backend, so that the flag is asserted
        # where the code under test actually writes it rather than on a mock call.
        return FxAsyncResult(id=str(uuid4()), app=ut_app)

    def test_a_declared_forget_is_ignored_without_waiting(self, child_result):
        # the child would drop its results at its own postrun, leaving nothing for
        # the enqueuing task to read back from the result it was handed.
        _enqueue_child(child_result, declares_forget=True)

        assert not _is_flagged_to_forget(child_result)

    def test_the_enqueuing_task_can_still_ask_to_forget(self, child_result):
        _enqueue_child(child_result, declares_forget=True, forget=True)

        assert _is_flagged_to_forget(child_result)

    def test_nothing_is_forgotten_when_nothing_declares_it(self, child_result):
        _enqueue_child(child_result, declares_forget=False)

        assert not _is_flagged_to_forget(child_result)

    def test_the_enqueuing_task_can_forget_a_remembered_task(self, child_result):
        _enqueue_child(child_result, declares_forget=False, forget=True)

        assert _is_flagged_to_forget(child_result)


class TestBlockingEnqueueChildForget:
    """A blocking child is forgotten by the enqueuing task instead of flagging itself."""

    @pytest.fixture
    def child_result(self) -> mock.Mock:
        # blocking snapshots the child's results into an FxEagerResult before
        # forgetting them, which a mock supplies without a task having really run.
        return mock.Mock()

    def test_a_declared_forget_is_the_default(self, child_result):
        task = _enqueue_child(child_result, declares_forget=True, block=True)

        task.forget_specific_children_results.assert_called_once_with([child_result])

    def test_the_enqueuing_task_can_keep_the_results(self, child_result):
        task = _enqueue_child(child_result, declares_forget=True, block=True, forget=False)

        assert not task.forget_specific_children_results.called

    def test_nothing_is_forgotten_when_nothing_declares_it(self, child_result):
        task = _enqueue_child(child_result, declares_forget=False, block=True)

        assert not task.forget_specific_children_results.called


def _enqueue_many(declares_forget: bool, **kwargs) -> tuple[mock.Mock, mock.Mock]:
    """Enqueue one chain that declares (or doesn't) forget, and report what was done."""
    chain = mock.Mock(spec=SignatureX)
    chain.declares_forget.return_value = declares_forget

    task = mock.Mock(spec=FireXTask)
    # part of enqueue_many's own logic rather than a collaborator, so let it run.
    task._forget_waited_chains = partial(FireXTask._forget_waited_chains, task)
    child_result = task.enqueue_child.return_value

    with mock.patch.object(ManyFxAsyncResults, 'wait_for_all'):
        FireXTask.enqueue_many(task, [chain], **kwargs)
    return task, child_result


class TestEnqueueManyForget:
    """
        enqueue_many always enqueues non-blocking to keep its chains parallel, so it
        has to forget them itself once it has waited rather than let them self-forget.
    """

    def test_a_waited_chain_is_forgotten_by_the_enqueuing_task(self):
        task, child_result = _enqueue_many(declares_forget=True, block=True)

        assert task.enqueue_child.call_args.kwargs['forget'] is False
        task.forget_specific_children_results.assert_called_once_with([child_result])

    def test_raising_on_failure_waits_and_so_forgets_too(self):
        task, child_result = _enqueue_many(declares_forget=True, raise_on_failure=True)

        task.forget_specific_children_results.assert_called_once_with([child_result])

    def test_a_declared_forget_is_ignored_without_waiting(self):
        task, _child_result = _enqueue_many(declares_forget=True)

        # nothing waits for these, so enqueue_child is left to decide, and declines.
        assert task.enqueue_child.call_args.kwargs['forget'] is None
        assert not task.forget_specific_children_results.called

    def test_the_enqueuing_task_can_keep_the_results(self):
        task, _child_result = _enqueue_many(declares_forget=True, block=True, forget=False)

        assert not task.forget_specific_children_results.called

    def test_the_enqueuing_task_can_forget_a_remembered_chain(self):
        task, child_result = _enqueue_many(declares_forget=False, block=True, forget=True)

        task.forget_specific_children_results.assert_called_once_with([child_result])

    def test_nothing_is_forgotten_when_nothing_declares_it(self):
        task, _child_result = _enqueue_many(declares_forget=False, block=True)

        assert not task.forget_specific_children_results.called


def _child_result(forgotten: bool) -> mock.Mock:
    child = mock.Mock(spec=FxAsyncResult)
    child.fx_is_forgotten.return_value = forgotten
    child.fx_logging_name.return_value = 'forgotten' if forgotten else 'waited'
    return child


def _wait_for_specific_children(child_results) -> list[mock.Mock]:
    """Wait for the given children, and report which of them were waited on."""
    task = mock.Mock(spec=FireXTask)

    with mock.patch.object(
        ManyFxAsyncResults, 'wait_for_all', autospec=True,
    ) as wait_for_all:
        FireXTask.wait_for_specific_children(task, child_results)

    if not wait_for_all.called:
        return []
    waited_on, = wait_for_all.call_args.args
    return list(waited_on)


class TestWaitingForForgottenChildren:
    """There is nothing left to wait for once a child has dropped its own results."""

    def test_a_forgotten_child_is_not_waited_for(self):
        assert _wait_for_specific_children([_child_result(forgotten=True)]) == []

    def test_the_remaining_children_are_still_waited_for(self):
        waited = _child_result(forgotten=False)

        assert _wait_for_specific_children([_child_result(forgotten=True), waited]) == [waited]

    def test_a_single_child_is_accepted_as_well_as_a_list(self):
        waited = _child_result(forgotten=False)

        assert _wait_for_specific_children(waited) == [waited]
        assert _wait_for_specific_children(_child_result(forgotten=True)) == []

