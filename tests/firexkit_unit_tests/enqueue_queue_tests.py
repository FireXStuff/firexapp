"""
The queue a signature is enqueued on, resolved against the running task.
"""

import contextlib
from unittest import mock

import pytest
from celery._state import pop_current_task, push_current_task
from celery.app.task import Task
from celery.canvas import Signature

from firexkit.chain import AUTO_QUEUE
from firexkit.firex_celery import FireXCelery
from firexkit.task import FireXTask, SchedulingDeadlockException


@pytest.fixture
def running_on(ut_app: FireXCelery):
    """Run the body as though celery were executing a task on `worker`."""

    @contextlib.contextmanager
    def _running_on(worker: str, routing_key: str, base=FireXTask):
        @ut_app.task(base=base, bind=True)
        def running(self):
            pass

        running.request.hostname = worker
        running.request.delivery_info = {"routing_key": routing_key}
        push_current_task(running)
        try:
            yield
        finally:
            pop_current_task()

    return _running_on


@pytest.fixture
def enqueued(ut_app: FireXCelery):
    """Enqueue a signature without reaching a broker, and report its queue."""

    @ut_app.task(base=FireXTask)
    def enqueue_me():
        pass

    def _enqueued(enqueue: str = "enqueue", **kwargs) -> str | None:
        sig = enqueue_me.s()
        with mock.patch.object(Signature, "apply_async"):
            getattr(sig, enqueue)(**kwargs)
        return sig.options.get("queue")

    return _enqueued


class TestAutoQueue:
    """'auto' routes back to the worker running the enqueuing task."""

    def test_a_task_on_mc_keeps_its_children_on_mc(self, running_on, enqueued):
        with running_on("mc@host1", routing_key="mc"):
            assert enqueued(queue=AUTO_QUEUE) == "mc"

    def test_a_task_off_mc_routes_to_its_host_worker(self, running_on, enqueued):
        with running_on("master@host1", routing_key="master"):
            # deliberately not a bare 'host1': only sandbox workers consume that,
            # and then only for backwards compatibility.
            assert enqueued(queue=AUTO_QUEUE) == "worker@host1"

    def test_outside_a_task_the_queue_is_left_to_celery(self, enqueued):
        # No worker to route back to, so the task's own queue -- else
        # task_default_queue -- decides.
        assert enqueued(queue=AUTO_QUEUE) is None

    def test_a_plain_celery_task_is_not_asked_to_resolve(self, running_on, enqueued):
        with running_on("master@host1", routing_key="master", base=Task):
            assert enqueued(queue=AUTO_QUEUE) is None


class TestExplicitQueue:
    """Resolution leaves queues that were actually asked for alone."""

    def test_an_explicit_queue_is_honoured(self, running_on, enqueued):
        with running_on("master@host1", routing_key="master"):
            assert enqueued(queue="mc") == "mc"

    def test_no_queue_stays_no_queue(self, running_on, enqueued):
        with running_on("mc@host1", routing_key="mc"):
            assert enqueued() is None

    def test_master_cannot_enqueue_onto_its_own_queue(self, running_on, enqueued):
        # the deadlock enqueue_child has always refused, now refused for
        # signature-level enqueues too.
        with (
            running_on("master@host1", routing_key="master"),
            pytest.raises(SchedulingDeadlockException),
        ):
            enqueued(queue="master")


class TestEnqueueAndExtractQueue:
    def test_it_defaults_to_the_running_worker(self, running_on, enqueued):
        with running_on("master@host1", routing_key="master"):
            assert enqueued("enqueue_and_extract") == "worker@host1"

    def test_an_explicit_queue_is_still_honoured(self, running_on, enqueued):
        with running_on("master@host1", routing_key="master"):
            assert enqueued("enqueue_and_extract", queue="mc") == "mc"
