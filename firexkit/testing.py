from __future__ import annotations

from typing import Any, Iterable, Optional
from uuid import uuid4

from celery import Celery

from firexkit.result import FxAsyncResult


class MockFxAsyncResult(FxAsyncResult):
    def __init__(
        self,
        result: Any=None,
        *,
        state: Any=None,
        successful: bool=True,
        children: Optional[Iterable[MockFxAsyncResult]]=None,
        parent: Optional[MockFxAsyncResult]=None,
        name: Optional[str]='mock_service',
        id: Optional[str]=None,
        app: Optional[Celery]=None,
    ):
        super().__init__(id=id or str(uuid4()), app=app or Celery(), parent=parent)
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