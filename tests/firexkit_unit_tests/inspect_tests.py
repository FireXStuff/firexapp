"""What firexkit makes of the queues celery's workers reply they consume."""

import dataclasses
from types import SimpleNamespace
from typing import Any

import pytest

from firexkit.firex_worker import FxBuiltinQueues, FxWorkerHostName
from firexkit.inspect import InspectedQueue

_HOST = "some-ad-hostname"


def _celery_queue(name: str) -> dict[str, Any]:
    """One queue in the shape celery's inspect API reports it.

    Celery describes queues in RabbitMQ/AMQP terms, so most of this means
    nothing to the modelling and is here to prove it gets past it.
    """
    return {
        "name": name,
        "exchange": {
            "name": name,
            "type": "direct",
            "arguments": None,
            "durable": True,
            "passive": False,
            "auto_delete": False,
            "delivery_mode": None,
            "no_declare": False,
        },
        "routing_key": name,
        "queue_arguments": None,
        "binding_arguments": None,
        "consumer_arguments": None,
        "durable": True,
        "exclusive": False,
        "auto_delete": False,
        "no_ack": False,
        "alias": None,
        "bindings": [],
        "no_declare": None,
        "expires": None,
        "message_ttl": None,
        "max_length": None,
        "max_length_bytes": None,
        "max_priority": None,
    }


@dataclasses.dataclass
class _UtCeleryCluster:
    """The workers ``app.control.inspect()`` reaches, in memory.

    A worker that isn't there simply doesn't reply, so what a caller can and
    can't tell apart is what it would be against a real broker. Which
    destinations were asked is recorded, since that is how celery is addressed.
    """

    queues_by_destination: dict[str, list[str]] = dataclasses.field(
        default_factory=dict
    )
    inspected_destinations: list[Any] = dataclasses.field(default_factory=list)

    def add_worker(self, destination: FxWorkerHostName, queues: list[str]) -> None:
        self.queues_by_destination[str(destination)] = queues

    def inspect(self, destination=None, **_kwargs):
        self.inspected_destinations.append(destination)
        if destination is None:
            replying = dict(self.queues_by_destination)  # a broadcast reaches all.
        else:
            replying = {
                d: self.queues_by_destination[d]
                for d in destination
                if d in self.queues_by_destination
            }
        return SimpleNamespace(
            active_queues=lambda: {
                d: [_celery_queue(q) for q in queues] for d, queues in replying.items()
            },
        )


@pytest.fixture(name="cluster")
def _cluster(ut_app):
    """An in-memory cluster installed as what the app inspects with.

    app.control is a (settable) cached_property, so this is as low as the
    faking can go without a broker -- the real one needs a live broker just to
    be constructed.
    """
    cluster = _UtCeleryCluster()
    ut_app.control = cluster
    try:
        yield cluster
    finally:
        del ut_app.control


def _worker(queue_name: str, spawn_group: str | None = None) -> FxWorkerHostName:
    return FxWorkerHostName(
        queue_name=queue_name,
        spawn_group=spawn_group,
        host=_HOST,
    )


def test_a_workers_reply_is_modelled_and_keyed_by_its_name(ut_app, cluster):
    master = _worker("master", "g1")
    cluster.add_worker(master, queues=["master:g1", f"shutdown@{_HOST}"])

    queues_by_worker = InspectedQueue.inspect_active_queues(celery_app=ut_app)

    assert queues_by_worker == {
        master: [
            InspectedQueue(name="master:g1", routing_key="master:g1"),
            InspectedQueue(name=f"shutdown@{_HOST}", routing_key=f"shutdown@{_HOST}"),
        ]
    }


def test_destinations_reach_celery_as_worker_names(ut_app, cluster):
    master = _worker("master", "g1")
    subworker = _worker("worker", "g1")
    cluster.add_worker(master, queues=["master:g1"])
    cluster.add_worker(subworker, queues=["worker:g1"])

    assert set(
        InspectedQueue.inspect_active_queues(
            celery_app=ut_app,
            destinations=[master, subworker],
        )
    ) == {master, subworker}
    assert cluster.inspected_destinations == [
        (f"master:g1@{_HOST}", f"worker:g1@{_HOST}")
    ]


def test_every_worker_answers_a_broadcast(ut_app, cluster):
    cluster.add_worker(_worker("master"), queues=["master"])
    cluster.add_worker(_worker("worker"), queues=["worker"])

    assert InspectedQueue.inspect_active_queue_names(celery_app=ut_app) == {
        "master",
        "worker",
    }
    assert cluster.inspected_destinations == [None]


def test_a_worker_that_does_not_answer_is_absent(ut_app, cluster):
    """Absent is not the same as answering that it consumes nothing: only the
    second one says anything about what the worker is doing."""
    idle = _worker("master", "g1")
    cluster.add_worker(idle, queues=[])
    silent = _worker("master", "g2")

    queues_by_worker = InspectedQueue.inspect_active_queues(
        celery_app=ut_app,
        destinations=[idle, silent],
    )

    assert queues_by_worker == {idle: []}
    assert (
        InspectedQueue.inspect_active_queues_single_destination(
            celery_app=ut_app,
            destination=silent,
        )
        is None
    )
    assert (
        InspectedQueue.inspect_active_queues_single_destination(
            celery_app=ut_app,
            destination=idle,
        )
        == []
    )


def test_a_queue_celery_cannot_describe_costs_only_itself(ut_app, caplog):
    """One unreadable queue is no reason to lose sight of the rest."""
    master = _worker("master")
    ut_app.control = SimpleNamespace(
        inspect=lambda **_kwargs: SimpleNamespace(
            active_queues=lambda: {
                str(master): [{"no": "name"}, _celery_queue("master")],
            },
        ),
    )
    try:
        assert InspectedQueue.inspect_active_queues(celery_app=ut_app) == {
            master: [InspectedQueue(name="master", routing_key="master")]
        }
    finally:
        del ut_app.control
    assert "while validating Celery queue" in caplog.text


@pytest.mark.parametrize(
    "queue_name, is_shutdown",
    [
        (f"shutdown@{_HOST}", True),  # every master on the host consumes this one
        (f"shutdown:g1@{_HOST}", True),  # and only one master consumes this one
        ("shutdown", True),
        (f"shutdownish@{_HOST}", False),
        (f"master@{_HOST}", False),
        (_HOST, False),
    ],
)
def test_which_queues_retire_a_worker_and_which_deliver_the_retiring(
    queue_name, is_shutdown
):
    """Shutdown queues are a family -- one per host, one per master on it --
    that a worker has to keep consuming to be told to stop."""
    assert (
        InspectedQueue(name=queue_name).is_builtin_queue(FxBuiltinQueues.SHUTDOWN)
        is is_shutdown
    )
