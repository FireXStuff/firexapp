"""
The task ids that forgetting must skip are tracked in a set per category, so that
collecting them costs one read of one key instead of a scan of the whole keyspace.
"""

from functools import partial
from types import SimpleNamespace

import pytest

from firexkit.firex_celery import FireXCelery
from firexkit.task import (
    REDIS_DB_KEY_FOR_CACHE_ENABLED_UIDS,
    REDIS_DB_KEY_FOR_ENQUEUE_ONCE_UIDS,
    add_cache_enabled_uid_to_db,
    add_enqueue_child_once_uid_to_db,
    get_current_cache_enabled_uids,
    get_current_enqueue_child_once_uids,
)
from firexkit.testing import UtClient


@pytest.fixture(
    params=[
        (add_cache_enabled_uid_to_db, get_current_cache_enabled_uids),
        (add_enqueue_child_once_uid_to_db, get_current_enqueue_child_once_uids),
    ],
    ids=["cache_enabled", "enqueue_once"],
)
def index(request, ut_app: FireXCelery) -> SimpleNamespace:
    """Each category's add/get pair, bound to a throwaway backend."""
    add, get = request.param
    return SimpleNamespace(
        add=partial(add, ut_app.backend),
        get=partial(get, ut_app.backend),
    )


def test_nothing_is_tracked_before_anything_is_added(index):
    assert index.get() == set()


def test_added_ids_are_reported_back(index):
    index.add("uid-1")
    index.add("uid-2")

    assert index.get() == {"uid-1", "uid-2"}


def test_adding_the_same_id_twice_reports_it_once(index):
    index.add("uid-1")
    index.add("uid-1")

    assert index.get() == {"uid-1"}


def test_the_categories_are_tracked_separately():
    assert REDIS_DB_KEY_FOR_CACHE_ENABLED_UIDS != REDIS_DB_KEY_FOR_ENQUEUE_ONCE_UIDS


def test_collecting_the_ids_reads_one_key(index, ut_client: UtClient):
    for i in range(10):
        index.add(f"uid-{i}")

    before = ut_client.call_count("smembers")
    assert len(index.get()) == 10

    # the point of the set: no scan of the keyspace, however many ids there are.
    assert ut_client.call_count("smembers") == before + 1
    assert ut_client.call_count("keys") == 0
