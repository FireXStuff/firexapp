import pytest
from redis.connection import Encoder

from firexkit.testing import UtClient

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
