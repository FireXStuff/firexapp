from unittest import TestCase, mock

import pytest
from celery.exceptions import BackendGetMetaError
from redis.connection import Encoder

from firexkit.testing import UtBackend, UtClient

# What redis-py sends redis for a value, and so what redis hands back for it.
_REDIS_ENCODER = Encoder(
    encoding="utf-8",
    encoding_errors="strict",
    decode_responses=False,
)


@pytest.mark.parametrize(
    "value",
    [b"36000", "36000", 36000, 2422892.0002548695, "naïve"],
    ids=["bytes", "str", "int", "float", "non-ascii"],
)
def test_set_stores_what_redis_would(value):
    client = UtClient()

    client.set("key", value)

    assert client.get("key") == _REDIS_ENCODER.encode(value)


class UtBackendTests(TestCase):
    def test_lookup_errors_are_not_disguised_as_missing_results(self):
        backend = UtBackend(UtClient())
        failure = RuntimeError("store unavailable")

        with (
            mock.patch.object(backend.client, "get", side_effect=failure) as get,
            self.assertRaises(RuntimeError) as raised,
        ):
            backend._ensure_retryable(
                backend._get_task_meta_for,
                task_id="task-id",
                fallback_exc=BackendGetMetaError,
                fallback_msg="failed to get meta",
                max_retries=3,
            )

        self.assertIs(raised.exception, failure)
        get.assert_called_once_with(backend.get_key_for_task("task-id"))
