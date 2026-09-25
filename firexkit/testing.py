from __future__ import annotations

import dataclasses
import math
import time
from collections.abc import Iterable
from typing import Any, ClassVar
from uuid import uuid4

import celery.states
from celery import Celery

from firexapp.engine.default_celery_config import FxEnvVars
from firexkit.firex_celery import FireXCelery
from firexkit.result import FxAsyncResult


def ut_celery_app(*args, **kwargs) -> FireXCelery:
    kwargs["set_as_current"] = False
    kwargs.setdefault("fx_env", FxEnvVars.create_no_task_exec_fx_env())
    return FireXCelery(*args, **kwargs)


class MockFxAsyncResult(FxAsyncResult):
    def __init__(
        self,
        result: Any = None,
        *,
        state: Any = None,
        successful: bool = True,
        children: Iterable[MockFxAsyncResult] | None = None,
        parent: MockFxAsyncResult | None = None,
        name: str | None = "mock_service",
        id: str | None = None,
        app: Celery | None = None,
    ):
        super().__init__(
            id=id or str(uuid4()), app=app or ut_celery_app(), parent=parent
        )
        self._state = state
        self._result = result
        self._successful = successful
        self._children = list(children or [])
        self._name = name

    @classmethod
    def set_heritage(cls, parent: MockFxAsyncResult, child: MockFxAsyncResult) -> None:
        child.parent = parent
        parent._children.append(child)

    @property
    def state(self):
        return self._state() if callable(self._state) else self._state

    @property
    def result(self):
        return self._result() if callable(self._result) else self._result

    @property
    def name(self):
        return self._name

    def _get_task_meta(self):
        return {"children": self._children}

    def successful(self):
        return self._successful


class UtClient:
    """
    Stand-in for the broker's redis client that keeps everything in memory.

    Only the operations FireX actually issues are implemented, and only for
    string/bytes values, which are stored (and handed back) encoded the way
    redis does. Every operation is counted, so a test can assert on how much
    broker traffic the code under test produced via :meth:`call_count`.
    """

    def __init__(self):
        self._store = {}
        self._expiries = {}
        self._call_counts = {}

    def call_count(self, name):
        return self._call_counts.get(name, 0)

    def _inc_count(self, name):
        try:
            self._call_counts[name] += 1
        except KeyError:
            self._call_counts[name] = 1

    @staticmethod
    def _encode(v):
        # only supports strings and bytes (returned as is) for now
        return v.encode() if not isinstance(v, bytes) else v

    def _drop_if_expired(self, encoded_key):
        """Redis drops an expired key lazily, when something next asks for it."""
        deadline = self._expiries.get(encoded_key)
        if deadline is not None and deadline <= time.monotonic():
            self._store.pop(encoded_key, None)
            del self._expiries[encoded_key]

    def get(self, key):
        self._inc_count("get")
        encoded_key = self._encode(key)
        self._drop_if_expired(encoded_key)
        try:
            return self._store[encoded_key]
        except KeyError:
            return None

    def set(self, key, value):
        self._inc_count("set")
        encoded_key = self._encode(key)
        self._store[encoded_key] = self._encode(str(value))
        # redis discards any time to live the key had when it is set again.
        self._expiries.pop(encoded_key, None)
        return True

    def expire(self, key, seconds):
        """Give a key a time to live, reporting whether there was a key to give it to.

        A time to live that has already run out deletes the key outright,
        as redis does rather than leaving it to be reaped later.
        """
        self._inc_count("expire")
        encoded_key = self._encode(key)
        self._drop_if_expired(encoded_key)
        if encoded_key not in self._store:
            return False
        if seconds <= 0:
            del self._store[encoded_key]
            self._expiries.pop(encoded_key, None)
        else:
            self._expiries[encoded_key] = time.monotonic() + seconds
        return True

    def ttl(self, key):
        """Seconds left on a key, with redis's two sentinels.

        -2 for a key that isn't there (or whose time to live has run out), and
        -1 for one that is there with no time to live set.
        """
        self._inc_count("ttl")
        encoded_key = self._encode(key)
        self._drop_if_expired(encoded_key)
        if encoded_key not in self._store:
            return -2
        deadline = self._expiries.get(encoded_key)
        if deadline is None:
            return -1
        return math.ceil(deadline - time.monotonic())

    def incr(self, key):
        self._inc_count("incr")
        encoded_key = self._encode(key)
        value = int(self._store.get(encoded_key, b"0")) + 1
        self._store[encoded_key] = self._encode(str(value))
        return value

    def mget(self, keys):
        self._inc_count("mget")
        encoded_keys = [self._encode(key) for key in keys]
        for encoded_key in encoded_keys:
            self._drop_if_expired(encoded_key)
        return [self._store.get(encoded_key) for encoded_key in encoded_keys]

    def rpush(self, key, value):
        self._inc_count("rpush")
        values = self._store.setdefault(self._encode(key), [])
        values.append(self._encode(value))
        return len(values)

    def lrange(self, key, start, end):
        self._inc_count("lrange")
        values = self._store.get(self._encode(key), [])
        return values[start:] if end == -1 else values[start : end + 1]

    def sadd(self, key, *members):
        self._inc_count("sadd")
        values = self._store.setdefault(self._encode(key), set())
        encoded = {self._encode(m) for m in members}
        # redis reports how many members this added, so re-adding one counts as none.
        added = len(encoded - values)
        values |= encoded
        return added

    def smembers(self, key):
        self._inc_count("smembers")
        return set(self._store.get(self._encode(key), set()))

    def hget(self, key, subkey):
        self._inc_count("hget")
        try:
            return self._store[self._encode(key)][self._encode(subkey)]
        except KeyError:
            return None

    def hgetall(self, key):
        self._inc_count("hgetall")
        try:
            return self._store[self._encode(key)]
        except KeyError:
            return None

    def setnx(self, key, value):
        self._inc_count("setnx")
        encoded_key = self._encode(key)
        self._drop_if_expired(encoded_key)
        if encoded_key in self._store:
            return False

        self._store[encoded_key] = self._encode(value)
        return True

    def hmset(self, key, d):
        self._inc_count("hmset")
        d_en = {self._encode(k): self._encode(v) for k, v in d.items()}

        try:
            self._store[self._encode(key)] |= d_en
        except KeyError:
            self._store[self._encode(key)] = d_en

        return True

    def hset(self, key, member, val):
        self._inc_count("hset")
        fields = self._store.setdefault(self._encode(key), {})
        encoded_member = self._encode(member)
        # redis reports how many fields this added, so overwriting one counts as none.
        added = int(encoded_member not in fields)
        fields[encoded_member] = self._encode(val)
        return added

    def hsetnx(self, key, member, val):
        self._inc_count("hsetnx")

        if self._encode(key) not in self._store:
            self._store[self._encode(key)] = {}

        if self._encode(member) in self._store[self._encode(key)]:
            return False

        self._store[self._encode(key)][self._encode(member)] = self._encode(val)
        return True

    def delete(self, key):
        self._inc_count("delete")
        encoded_key = self._encode(key)
        self._expiries.pop(encoded_key, None)
        del self._store[encoded_key]

    def reset(self):
        self._store.clear()
        self._expiries.clear()
        self._call_counts.clear()

    @property
    def backend(self):
        return self

    @property
    def client(self):
        return self


@dataclasses.dataclass
class UtBackend:
    """Stand-in for the celery result backend, storing to a :class:`UtClient`."""

    client: UtClient

    thread_safe: bool = True

    READY_STATES: ClassVar[frozenset[str]] = celery.states.READY_STATES

    def remove_pending_result(self, _):
        pass

    # Mirrors celery.backends.redis.RedisBackend.get()/set(), which delegate
    # to self.client directly (not through self.backend.client.*). Production
    # code (e.g. FireXCelery.set_attr_in_conf_and_backend()) calls
    # self.backend.get()/self.backend.set() the same way.
    def get(self, key):
        return self.client.get(key)

    def set(self, key, value):
        return self.client.set(key, value)


def ut_backed_celery_app(*args, **kwargs) -> FireXCelery:
    """A :func:`ut_celery_app` whose backend stores to memory instead of redis."""
    app = ut_celery_app(*args, **kwargs)
    # the app is built for a single test, so there's nothing to restore afterwards.
    app._backend = UtBackend(UtClient())
    return app
