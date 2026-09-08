from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import uuid4

from celery import Celery

from firexapp.engine.default_celery_config import FxEnvVars
from firexkit.firex_celery import FireXCelery
from firexkit.result import FxAsyncResult


def ut_celery_app(*args, **kwargs) -> FireXCelery:
    kwargs['set_as_current'] = False
    kwargs.setdefault('fx_env', FxEnvVars.create_no_task_exec_fx_env())
    return FireXCelery(*args, **kwargs)


class MockFxAsyncResult(FxAsyncResult):
    def __init__(
        self,
        result: Any=None,
        *,
        state: Any=None,
        successful: bool=True,
        children: Iterable[MockFxAsyncResult] | None=None,
        parent: MockFxAsyncResult | None=None,
        name: str | None='mock_service',
        id: str | None=None,
        app: Celery | None=None,
    ):
        super().__init__(id=id or str(uuid4()), app=app or ut_celery_app(), parent=parent)
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
        return {'children': self._children}

    def successful(self):
        return self._successful