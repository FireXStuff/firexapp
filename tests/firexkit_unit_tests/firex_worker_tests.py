import pytest

from firexkit.firex_worker import (
    FxWorkerHostName,
    FxWorkerId,
    FxWorkerName,
    FxWorkerTypes,
)

_HOST = "some-ad-hostname"


@pytest.mark.parametrize(
    "worker_id, expected_str",
    [
        (
            FxWorkerId(
                queue_name="master", spawn_group="g2", host=_HOST, uniq_slug="2e51aeeb"
            ),
            f"master:g2:2e51aeeb@{_HOST}",
        ),
        (
            FxWorkerId(queue_name="master", host=_HOST, uniq_slug="2e51aeeb"),
            f"master::2e51aeeb@{_HOST}",
        ),
        (
            # a spawn group can itself contain the delimiter.
            FxWorkerId(
                queue_name="worker",
                spawn_group="g2:3",
                host=_HOST,
                uniq_slug="2e51aeeb",
            ),
            f"worker:g2:3:2e51aeeb@{_HOST}",
        ),
    ],
)
def test_worker_id_str_round_trip(worker_id, expected_str):
    assert str(worker_id) == expected_str
    assert FxWorkerId.fx_worker_id_from_str(expected_str) == worker_id


@pytest.mark.parametrize(
    "not_an_id",
    [
        f"master@{_HOST}",  # a name, not an ID
        f"master:g2@{_HOST}",  # a name with a spawn group
        f"master::2e51aeeb@{_HOST}@extra",
        "master::2e51aeeb",  # no host
        f"master::@{_HOST}",  # no uniq slug
    ],
)
def test_worker_id_from_non_id_str(not_an_id):
    with pytest.raises(ValueError):
        FxWorkerId.fx_worker_id_from_str(not_an_id)


def test_worker_id_must_have_uniq_slug():
    with pytest.raises(AssertionError):
        FxWorkerId(queue_name="master", host=_HOST, uniq_slug="")

    with pytest.raises(AssertionError):
        FxWorkerId(queue_name="master", host=_HOST, uniq_slug="has:delimiter")


def test_worker_ids_of_same_name_are_unique():
    name = FxWorkerName(queue_name="worker").as_host_worker(_HOST)
    assert name.as_worker_id() != name.as_worker_id()


def test_worker_id_keeps_name():
    name = FxWorkerName.fx_worker_name_from_str("master:g2").as_host_worker(_HOST)
    worker_id = name.as_worker_id()

    assert worker_id.as_host_worker_name() == name
    assert str(worker_id.as_host_worker_name()) == f"master:g2@{_HOST}"
    assert FxWorkerHostName.fx_worker_host_name_from_str(str(name)) == name


def test_empty_spawn_group_is_no_spawn_group():
    assert FxWorkerName.fx_worker_name_from_str("master:") == FxWorkerName(
        queue_name="master",
    )


@pytest.mark.parametrize(
    "worker_name, worker_type, expected",
    [
        ("master:g2", FxWorkerTypes.MASTER, True),
        ("master", FxWorkerTypes.MASTER, True),
        ("master:g2", FxWorkerTypes.WORKER, False),
        ("worker:g2", FxWorkerTypes.WORKER, True),
        ("mc", FxWorkerTypes.MC, True),
        ("not_a_queue", FxWorkerTypes.MASTER, False),
    ],
)
def test_is_worker_type(worker_name, worker_type, expected):
    name = FxWorkerName.fx_worker_name_from_str(worker_name)
    assert name.is_worker_type(worker_type) is expected
    assert name.as_host_worker(_HOST).is_worker_type(worker_type) is expected


def test_as_fx_worker_name_from_str():
    assert FxWorkerName.as_fx_worker_name("master:g2") == FxWorkerName(
        queue_name="master",
        spawn_group="g2",
    )


@pytest.mark.parametrize(
    "already_a_name",
    [
        FxWorkerName(queue_name="master", spawn_group="g2"),
        FxWorkerHostName(queue_name="master", spawn_group="g2", host=_HOST),
        FxWorkerId(
            queue_name="master", spawn_group="g2", host=_HOST, uniq_slug="2e51aeeb"
        ),
    ],
)
def test_as_fx_worker_name_passes_names_through(already_a_name):
    assert FxWorkerName.as_fx_worker_name(already_a_name) is already_a_name
