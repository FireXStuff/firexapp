# hack Celery AsyncResult generics e.g.AsyncResult[Optional[str]]
from __future__ import annotations

import uuid
import time
from collections import namedtuple, deque
import contextlib
from pprint import pformat
from typing import (
    Union, Optional, Iterable, Any, Callable, TypeVar,
    Generator, ClassVar, Generic, Iterator, Sequence
)
import dataclasses
import socket

from celery.app.base import Celery
from celery.result import AsyncResult
from celery.states import FAILURE, REVOKED, PENDING, STARTED, RECEIVED, RETRY, SUCCESS, READY_STATES
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy
import vine

from firexkit.broker import handle_broker_timeout
from firexkit import inspect as fx_inspect
from firexkit.revoke import RevokedRequests


RETURN_KEYS_KEY = '__task_return_keys'
DYNAMIC_RETURN = '__DYNAMIC_RETURN__'

RUN_RESULTS_NAME = 'chain_results'
RUN_UNSUCCESSFUL_NAME = 'unsuccessful_services'

logger = get_task_logger(__name__)

_CHECK_TASK_WORKER_FREQ = 600
_SLEEP_BETWEEN_ITERATIONS = 0.05

class ReturnsCodingException(Exception):
    pass


class FireXResults:

    @staticmethod
    def is_prev_task_result(value: Any) -> bool:
        if (
            isinstance(value, dict)
            and (chain_depth := value.get('chain_depth'))
        ):
            return isinstance(chain_depth, int) and chain_depth > 1
        return False

    @classmethod
    def task_returns_to_tuple(
        cls,
        return_keys: tuple[str, ...],
        result: Any,
    ) -> tuple[Any, ...]:
        results_tuple : tuple[Any, ...]
        if not return_keys and result in [ None, {} ]:
            #FIXME: should print error on no returns keys with returns.
            results_tuple = tuple()
        elif (
            # handle named tuples, they are a result, not all the results
            (
                type(result) != tuple
                and isinstance(result, tuple)
            )
            # handle case of singular result
            or not isinstance(result, tuple)
        ):
            results_tuple = (result,)
        else:
            results_tuple = result

        return results_tuple

    @classmethod
    def convert_result_tuple_to_dict(
        cls,
        return_keys: tuple[str, ...],
        results_tuple: tuple[Any, ...],
    ) -> dict[str, Any]:

        if len(return_keys) != len(results_tuple):
            raise ReturnsCodingException(
                f'Expected return keys {return_keys} (length {len(return_keys)}) in service results, '
                f'but found length: {len(results_tuple)}: {results_tuple}'
            )

        # time to process the multiple return values
        flat_results : dict[str, Any] = {}
        for k, v in zip(return_keys, results_tuple):
            if k == DYNAMIC_RETURN:
                if v:
                    if not isinstance(v, dict):
                        raise TypeError(
                            f'The value of the dynamic returns {k} must be a dictionary.'
                            f'Current return value {v} is of type {type(v).__name__}'
                        )
                    flat_results.update(v)
            else:
                flat_results[k] = v

        # Inject into the results the RETURN_KEYS
        if flat_results:
            flat_results[RETURN_KEYS_KEY] = tuple(flat_results.keys())
        return flat_results

    @staticmethod
    def returns(*args):
        """ The decorator is used to allow us to specify the keys of the
            dict that the task returns.

            This is used only to signal to the user the inputs and outputs
            of a task, and deduce what arguments are required for a chain.
        """
        if not args:
            raise ReturnsCodingException("@returns cannot be empty")
        if len(args) != len(set(args)):
            raise ReturnsCodingException("@returns cannot contain duplicate keys")

        def decorator(func):
            if type(func) is PromiseProxy:
                raise ReturnsCodingException("@returns must be applied to a function (before @app.task)")

            # Store the arguments of the decorator as a function attribute
            undecorated = func
            while ( wrapped := getattr(undecorated, '__wrapped__', None) ):
                undecorated = wrapped
            undecorated._decorated_return_keys = args

            return func

        return decorator

_DEFAULT_AR_QUERY_TIMEOUT = 15 * 60
_DEFAULT_AR_RETRY_DELAY = 1


def _backend_result_to_str(backend_res: Union[None, bytes, bytearray]) -> str:
    if backend_res is None:
        return ''
    return backend_res.decode()


_FX_STARTED_STATES = set(READY_STATES) | {STARTED, RETRY}

WaitLoopCallBack = namedtuple('WaitLoopCallBack', ['func', 'frequency', 'kwargs'])

ARR = TypeVar('ARR')
R = TypeVar('R')

class FxAsyncResult(AsyncResult, Generic[ARR]):

    # tracked only if enable_ar_tracking is set
    _ARS_BY_ID : ClassVar[Optional[dict[str, 'FxAsyncResult']]] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._fx_name : Optional[str] = None
        self._fx_parent : Optional[FxAsyncResult] = None
        self._fx_parent_id : Optional[str] = None
        self._fx_queue : Optional[str] = None
        self._fx_terminal_state: Optional[str] = None
        self._fx_seen_queue: Optional[str] = None
        self._fx_hostname: Optional[str] = None

        # This is the parent if the AR is part of a chain,
        # it is not the parent task the caused this AR to be created.
        self.parent : Optional[FxAsyncResult]
        self.children : Optional[Iterable[FxAsyncResult]]

        # AsyncResult objects cannot be in memory after the broker (i.e. backend) shutdowns, otherwise errors are
        # produced when they are garbage collected. We therefore track AsyncResults so
        # that disable_all_ar_backends can disable their references to the backend.
        if FxAsyncResult._ARS_BY_ID is not None:
            FxAsyncResult._ARS_BY_ID[self.id] = self

        from firexkit.firex_celery import FireXCelery
        self.app : FireXCelery

    @classmethod
    def enable_ar_tracking(cls):
        if cls._ARS_BY_ID is None:
            cls._ARS_BY_ID = {}

    @classmethod
    def disable_all_ar_backends(cls):
        for ar in (cls._ARS_BY_ID or {}).values():
            ar.backend = None

    def fx_get_name(
        self,
        timeout: int=_DEFAULT_AR_QUERY_TIMEOUT,
        retry_delay: int=_DEFAULT_AR_RETRY_DELAY,
    ) -> Optional[str]:
        if self._fx_name is None:
            if b_name := self.fx_backend_get_name(
                default='',
                timeout=timeout,
                retry_delay=retry_delay,
            ):
                self._fx_name = b_name
        return self._fx_name

    def fx_logging_name(self) -> str:
        return f'{self.fx_backend_get_name("") or ""}[{self.id}]'

    def fx_get_hostname(self) -> Optional[str]:
        if self._fx_hostname is None:
            info = handle_broker_timeout(lambda r: r.info, args=(self,))
            try:
                # NOTE: if the task completes after the check for state right above but before the call
                # to handle_broker_timeout(), the type of 'info' is whatever the task returned, not the internal
                # Celery dictionary we want. It can be an exception, or even a dictionary with a random 'hostname'.
                # In the latter case _is_worker_alive() will return False, but since we retry _is_worker_alive() that
                # should be fine -- this timing issue cannot happen twice for the same task.
                self._fx_hostname = info.get('hostname')
            except AttributeError:
                pass
        return self._fx_hostname

    def firex_serializable(self) -> str:
        if self.failed() and (task_name := self.fx_get_name()):
            if isinstance(self.result, Exception):
                failure = first_non_chain_interrupted_exception(self.result)
            else:
                failure = self.result
            return f'{task_name.split(".")[-1]} failed: {failure}'
        return repr(self)

    def _fx_get_backend_attr(
        self,
        key_name: str,
        timeout: int,
        retry_delay: int,
        default: str='',
    ) -> str:
        try:
            maybe_name_bytes = handle_broker_timeout(
                self.app.backend.client.hget,
                args=(self.id, key_name),
                timeout=timeout,
                retry_delay=retry_delay,
            )
            return _backend_result_to_str(maybe_name_bytes)
        except AttributeError:
            return default

    def fx_backend_get_name(
        self,
        default=None,
        timeout: int=_DEFAULT_AR_QUERY_TIMEOUT,
        retry_delay: int=_DEFAULT_AR_RETRY_DELAY,
    ) -> str:
        # we have bugs around task names and its
        # tough to know what part is the workaround
        # and what part is the bug
        if default is None:
            default = self.id
        backend_name = self._fx_get_backend_attr(
            'name',
            default=default,
            timeout=timeout,
            retry_delay=retry_delay,
        )
        if not self._fx_name and backend_name != self.id:
            self._fx_name = backend_name
        return backend_name

    def fx_get_queue(self) -> str:
        if self._fx_queue is None:
            try:
                for r in self.get_chain_ancestors():
                    if queue := r._fx_get_backend_attr(
                        'queue',
                        timeout=_DEFAULT_AR_QUERY_TIMEOUT,
                        retry_delay=_DEFAULT_AR_RETRY_DELAY,
                    ):
                        self._fx_queue = queue
                        return self._fx_queue
            except AttributeError:
                logger.exception('Task queue info not supported for this broker')

        return self._fx_queue or ''

    def get_chain_head(self) -> 'FxAsyncResult':
        return list(self.get_chain_ancestors())[-1]

    def fx_seen_queue(self) -> bool:
        # use this when checking alive, including RECEIVED
        if not ( task_queue := self.fx_get_queue() ):
            logger.debug(f'Cannot get task queue for {self.fx_logging_name()}; assuming task is alive.')
            return False

        if not _was_queue_ready(self.app, task_queue):
            logger.debug(f'Queue "{task_queue}" for {self.fx_logging_name()} not seen yet; assuming task is alive.')
            return False
        return True

    def fx_get_parent(
        self,
        timeout: int=_DEFAULT_AR_QUERY_TIMEOUT,
        retry_delay: int=_DEFAULT_AR_RETRY_DELAY,
    ) -> Optional['FxAsyncResult']:
        if self._fx_parent is None:
            self._fx_parent = handle_broker_timeout(
                getattr,
                args=(self, 'parent'),
                timeout=timeout,
                retry_delay=retry_delay,
            )
        return self._fx_parent

    def fx_is_running(self) -> bool:
        return (
            self.app.task_id_has_prerun(self.id)
            and not self.app.task_id_has_postrun(self.id)
        )

    def fx_get_state(
        self,
        timeout: int=_DEFAULT_AR_QUERY_TIMEOUT,
        retry_delay: int=_DEFAULT_AR_RETRY_DELAY,
    ) -> str:
        return self._fx_terminal_state or handle_broker_timeout(
            getattr,
            args=(self, 'state'),
            timeout=timeout,
            retry_delay=retry_delay,
        ) or ''

    def fx_is_revoked(self) -> bool:
        # FIXME: some REVOKED checks did this before, is it still necessary?
        # Celery 5.0.1 has bugs where revoked tasks end up in RETRY state. This should be safe here
        # since FindFailureTest should never be re-tried.
        #   or async_result.state == RETRY
        return bool(
            self.fx_get_state() == REVOKED
            or RevokedRequests.is_revoked_uuid(self.id)
        )

    def fx_is_ready(
        self,
        timeout: int=_DEFAULT_AR_QUERY_TIMEOUT,
    ) -> bool:
        if self._fx_terminal_state is not None:
            return True
        state = self.fx_get_state(timeout=timeout)
        if (
            state
            and (
                state in self.backend.READY_STATES
                or RevokedRequests.is_revoked_uuid(self.id)
            )
        ):
            # nuts but this means PENDING can be terminal, sometimes!!
            self._fx_terminal_state = state
            return True
        return False

    def fx_get_parent_id(self) -> Optional[str]:
        if self._fx_parent_id is None:
            self._fx_parent_id =  self._fx_get_backend_attr(
                '_fx_parent_id',
                timeout=_DEFAULT_AR_QUERY_TIMEOUT,
                retry_delay=_DEFAULT_AR_RETRY_DELAY,
            ) or None
        return self._fx_parent_id

    @contextlib.contextmanager
    def update_parent_task_blocked_states(
        self,
        parent_id: Optional[str]=None,
    ):
        if not parent_id:
            if not self.fx_is_ready():
                parent_id = self.fx_get_parent_id()
            else:
                # disable blocking state change on parent since this ar
                # is already complete
                parent_id = None

        if (
            parent_id
            and not FxAsyncResult(parent_id, app=self.app).fx_is_ready()
        ):
            with self.app.events.default_dispatcher(
                hostname=self.fx_get_hostname() or socket.gethostname(),
            ) as d:
                d.send('task-blocked', uuid=parent_id)
                try:
                    yield
                finally:
                    try:
                        d.send('task-unblocked', uuid=parent_id)
                    except Exception:
                        pass
        else:
            yield

    def _handle_broker_timeout(
        self,
        callable_func: Callable[..., R],
        args: tuple[Any, ...]=tuple(),
        timeout=_DEFAULT_AR_QUERY_TIMEOUT,
        retry_delay=_DEFAULT_AR_RETRY_DELAY,
    ) -> R:
        return handle_broker_timeout(
            callable_func,
            args=args,
            timeout=timeout,
            retry_delay=retry_delay,
        )

    def get_chain_ancestors(
        self,
        max_parent_id: Optional[str]=None,
    ) -> Generator['FxAsyncResult', None, None]:
        parent : Optional['FxAsyncResult'] = self
        seen_ids : set[str] = set()
        while (
            parent
            and parent.id not in seen_ids
            and (
                max_parent_id is None
                or parent.id != max_parent_id
            )
        ):
            yield parent
            seen_ids.add(parent.id)
            parent = parent.fx_get_parent()

    def get_chain_ancestors_as_many(
        self,
        max_parent_id: Optional[str]=None,
    ) -> 'ManyFxAsyncResults':
        return ManyFxAsyncResults.fx_ars_from_list(
            list(self.get_chain_ancestors(max_parent_id))
        )

    def fx_is_failed(self) -> bool:
        if self._fx_terminal_state:
            return self._fx_terminal_state == FAILURE
        return self._handle_broker_timeout(
            self.failed,
        ) or False

    def fx_is_successful(self) -> bool:
        if self._fx_terminal_state:
            return self._fx_terminal_state == SUCCESS
        return self._handle_broker_timeout(
            self.successful,
        ) or False

    def _fx_raw_result(self) -> Union[Exception, dict[str, Any]]:
        return self._handle_broker_timeout(
            getattr,
            args=(self, 'result'),
        )

    def fx_get_success_result(self) -> dict[str, Any]:
        if not self.fx_is_successful():
            raise ValueError(f'Cannot get success result of {self.fx_logging_name()} with state {self.fx_get_state()}')
        r = self._fx_raw_result()
        assert not isinstance(r, Exception), f'{self.fx_logging_name()} with state {elf.fx_get_state()} unexpectedly had result: {r}'
        return r

    def fx_exception_result(self) -> Optional[Exception]:
        ex = self._fx_raw_result()
        return ex if isinstance(ex, Exception) else None

    def fx_forget(self):
        logger.debug(f'Forgetting result: {self.fx_logging_name()}')
        self._cache = None
        self.backend.forget(self.id)
        (self._ARS_BY_ID or {}).pop(self.id, None)
        # self.backend = None # needed, or does forget take care of it?

    def fx_wait_no_state_update(
        self,
        max_wait: Optional[float]=None,
        callbacks: Iterable[WaitLoopCallBack] = tuple(),
        log_msg: bool=True,
        start_time: Optional[float]=None,
        max_sleep: float=_SLEEP_BETWEEN_ITERATIONS * 20 * 15,  # Somewhat arbitrary,
        last_callback_time: Optional[dict[Callable, float]]=None,
        raise_on_failure=True,
    ) -> str:
        """
            Expected to only be called by infra, e.g. when multiple
            FxARs are waited on via ManyFxAsyncResults
        """
        if log_msg:
            logger.debug(f'-> Waiting for {self.fx_logging_name()} to complete')

        if start_time is None:
            start_time = time.monotonic()
        if last_callback_time is None:
            last_callback_time = {c.func: start_time for c in callbacks}

        result_state : Optional[str] = None
        try:
            _poll_for_ar_complete(
                self,
                start_time=start_time,
                max_wait=max_wait,
                max_sleep=max_sleep,
                callbacks=callbacks,
                last_callback_time=last_callback_time,
            )
            # If failure happened in a chain, raise from the failing task within the chain
            _check_for_failure_in_parents(self)

            result_state = self.fx_get_state()
            if result_state == REVOKED:
                # Wait for revoked tasks to actually finish running
                # Somewhat long max_wait in case a task does work when revoked, like
                # killing a child run launched by the task.
                ManyFxAsyncResults.fx_ars_from_single(self).wait_for_running(
                    max_wait=5*60
                )
                raise ChainRevokedException(
                    task_id=self.id,
                    task_name=self.fx_get_name(),
                )
            if result_state == PENDING:
                # Pending tasks can be in revoke list. State will still be PENDING.
                raise ChainRevokedPreRunException(self.id, self.fx_get_name())
            if result_state == FAILURE:
                raise _chain_interrupted_ex(self)
        except ChainInterruptedException as e:
            if raise_on_failure:
                raise e
            if log_msg:
                logger.warning(
                    f'Task {self.fx_logging_name()} failure caused by {first_non_chain_interrupted_exception(e)}'
                )
        finally:
            if log_msg and max_wait is None and result_state:
                logger.debug(f'-> Completed waiting for {self.fx_logging_name()} with state {result_state}')

        return result_state or self.fx_get_state()

    def fx_wait(
        self,
        max_wait: Optional[float]=None,
        callbacks: Iterable[WaitLoopCallBack] = tuple(),
        log_msg: bool=True,
        start_time: Optional[float]=None,
        max_sleep: float=_SLEEP_BETWEEN_ITERATIONS * 20 * 15,  # Somewhat arbitrary,
        last_callback_time: Optional[dict[Callable, float]]=None,
        raise_on_failure: bool=True,
        parent_id: Optional[str]=None,
    ) -> str:
        with self.update_parent_task_blocked_states(parent_id=parent_id):
            return self.fx_wait_no_state_update(
                max_wait=max_wait,
                callbacks=callbacks,
                log_msg=log_msg,
                start_time=start_time,
                max_sleep=max_sleep,
                last_callback_time=last_callback_time,
                raise_on_failure=raise_on_failure,
            )

    def get_many_results(self, return_keys: Sequence[str]) -> tuple[Any, ...]:
        assert return_keys, f'No return_keys supplied'
        self.fx_wait()
        return _get_results_tuple(self, return_keys=return_keys)

    def get_result_key(
        self,
        return_key: str,
        raise_on_failure=True,
    ) -> Any:
        self.fx_wait(raise_on_failure=raise_on_failure)
        # FIXME: fail if key not present and success!
        return self.get_many_results([return_key])[0]

    def legacy_extract_results(
        self,
        return_keys: Union[str, Sequence[str]],
        return_keys_only: bool = False,
        merge_children_results: bool = True,
        parent_id: Optional[str]=None,
        extract_from_parents=True,
    ) -> dict[str, Any]: # FIXME: split return types
        return get_results(
            self,
            return_keys=return_keys,
            return_keys_only=return_keys_only,
            merge_children_results=merge_children_results,
            parent_id=parent_id,
            extract_from_parents=extract_from_parents,
        )


class FxEagerResult(FxAsyncResult):
    """Taken from Celery EagerResult"""

    def __init__(
        self,
        id: Optional[str]=None,
        ret_value: Any=None,
        state: Optional[str]=None,
        app=None,
        traceback=None,
        fx_ar: Optional[FxAsyncResult]=None,
    ):
        if fx_ar is not None:
            self.id = fx_ar.id
            self._result = fx_ar._fx_raw_result()
            self._state = fx_ar.fx_get_state()
            self._traceback = fx_ar.traceback
            self.app = fx_ar.app

            self._fx_hostname = fx_ar._fx_hostname
            self._fx_name = fx_ar._fx_name
            self._fx_terminal_state = fx_ar._fx_terminal_state
        else:
            self.id = id or str(uuid.uuid4())
            self._result = ret_value
            assert state, f'state must be supplied when fx_ar is not'
            self._state = state
            self._traceback = traceback
            from firexkit.firex_celery import FireXCelery
            fx_app : Optional[FireXCelery] = app
            assert fx_app, f'app must be supplied when fx_ar is not'
            self.app = fx_app

        self.on_ready = vine.promise()
        self.on_ready(self)
        super().__init__(
            self.id,
            backend=self.app.backend,
            app=self.app,
            # parent=None,
        )
        self._cache = {
            'task_id': self.id,
            'result': self._result,
            'status': self._state,
            'traceback': self._traceback,
        }

    def _get_task_meta(self):
        return self._cache

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return (self.id, self._result, self._state, self._traceback)

    def __copy__(self):
        cls, args = self.__reduce__()
        return cls(*args)

    def ready(self):
        return True

    def forget(self):
        pass

    def revoke(self, *args, **kwargs):
        self._state = REVOKED

    def __repr__(self):
        return f'<FxEagerResult: {self.id}>'

    @property
    def result(self):
        return self._result

    @property
    def state(self):
        return self._state

    @property
    def traceback(self):
        return self.traceback


K = TypeVar('K')

@dataclasses.dataclass
class ManyFxAsyncResults(Generic[K]):

    _fx_ars_by_key: dict[K, FxAsyncResult]

    @classmethod
    def create_fx_ars(
        cls,
        results: Union[
            FxAsyncResult,
            list[FxAsyncResult],
            None,
        ],
    ):
        if isinstance(results, FxAsyncResult):
            results = [results]
        return cls.fx_ars_from_list(results or [])

    @classmethod
    def fx_ars_from_list(cls, fx_ars: Iterable[FxAsyncResult]) -> 'ManyFxAsyncResults[int]':
        return ManyFxAsyncResults(
            {i: ar for i, ar in enumerate(fx_ars)}
        )

    @classmethod
    def fx_ars_from_dict(cls, fx_ars_by_key: dict[K, FxAsyncResult]) -> 'ManyFxAsyncResults[K]':
        return ManyFxAsyncResults(dict(fx_ars_by_key))

    @classmethod
    def fx_ars_from_single(cls, fx_ar: FxAsyncResult) -> 'ManyFxAsyncResults[int]':
        return ManyFxAsyncResults({0: fx_ar})

    def __str__(self):
        return ", ".join(r.fx_logging_name() for r in self)

    def _get_running(self) -> 'ManyFxAsyncResults':
        return ManyFxAsyncResults(
            {
                k: ar
                for k, ar in self._fx_ars_by_key.items()
                if ar.fx_is_running()
            }
        )

    def __bool__(self):
        return bool(self._fx_ars_by_key)

    def __iter__(self) -> Iterator[FxAsyncResult]:
        return iter(self._fx_ars_by_key.values())

    def __len__(self):
        return len(self._fx_ars_by_key)

    def __getitem__(self, key):
        return self._fx_ars_by_key[key]

    def as_dict(self) -> dict[K, FxAsyncResult]:
        return dict(self._fx_ars_by_key)

    def revoke_non_ready(self, max_wait: int=2*60) -> 'ManyFxAsyncResults':
        """
            returns FxAsyncResult that were revoked.
        """
        revoked_ars : list[FxAsyncResult] = []
        for ar in self:
            if not ar.fx_is_ready():
                for chain_entry in ar.get_chain_ancestors():
                    if (
                        ( is_input_ar := (ar.id == chain_entry.id) )
                        or not chain_entry.fx_is_ready()
                    ):
                        chain_entry.revoke(terminate=True)
                        if not is_input_ar:
                            msg_detail = f' (in chain of {ar.fx_logging_name()})'
                        else:
                            msg_detail = ''
                        logger.info(
                            f'Revoked child {chain_entry.fx_logging_name()}{msg_detail}'
                        )
                        revoked_ars.append(chain_entry)
        many_revoked = ManyFxAsyncResults.fx_ars_from_list(revoked_ars)
        # wait for visible revoke completion
        many_revoked.wait_for_running(max_wait)
        return many_revoked

    def wait_for_any(
        self,
        max_wait: Optional[float]=None,
        callbacks: Iterable[WaitLoopCallBack] = tuple(),
        raise_on_failure: bool=True,
    ) -> FxAsyncResult:
        return next(
            self.get_as_completed(
                max_wait=max_wait,
                callbacks=callbacks,
                raise_on_failure=raise_on_failure,
            )
        )

    def get_as_completed(
        self,
        max_wait: Optional[float]=None,
        poll_max_wait: Optional[float]=None,
        callbacks: Iterable[WaitLoopCallBack] = tuple(),
        raise_on_failure: bool=True,
    ) -> Generator[FxAsyncResult, None, None]:
        poll_wait = poll_max_wait or 0.1
        max_poll_wait = 5 # arbitrary
        _warn_on_never_callback(callbacks, poll_wait)

        start_time = time.time()
        logger.debug(
            'Waiting for any of the following tasks to complete:\n'
            + '\n'.join([f'-> {r.fx_logging_name()}' for r in self]))
        if first_ar := next(iter(self), None):
            # assume all ars have same parent.
            with first_ar.update_parent_task_blocked_states():
                remaining_ars = ManyFxAsyncResults(self._fx_ars_by_key)
                while remaining_ars:
                    if max_wait and max_wait < time.time() - start_time:
                        raise WaitOnChainTimeoutError(
                            f'Results {remaining_ars} were still not ready after {max_wait} seconds'
                        )
                    for k, ar in dict(remaining_ars._fx_ars_by_key).items():
                        try:
                            ar.fx_wait_no_state_update(
                                max_wait=poll_wait,
                                log_msg=False,
                                callbacks=callbacks,
                                raise_on_failure=raise_on_failure,
                            )
                        except WaitOnChainTimeoutError:
                            poll_wait = _sleep_exponential_backoff(
                                poll_wait,
                                max_poll_wait)
                        else:
                            yield ar
                            logger.debug(f'--> {ar.fx_logging_name()} completed with state {ar.fx_get_state()}')
                            remaining_ars._fx_ars_by_key.pop(k, None)

    def wait_for_all(
        self,
        max_wait: Optional[float]=None,
        callbacks: Iterable[WaitLoopCallBack] = tuple(),
        log_msg: bool=True,
        raise_on_failure: bool=True,
    ) -> 'ManyFxAsyncResults[K]':
        failures : list[Exception] = []
        start_time = time.monotonic()
        last_callback_time = {c.func: start_time for c in callbacks}
        if first_ar := next(iter(self), None):
            # assume all ars have same parent.
            with first_ar.update_parent_task_blocked_states():
                for ar in self:
                    try:
                        ar.fx_wait_no_state_update(
                            log_msg=log_msg,
                            start_time=start_time,
                            max_wait=max_wait,
                            callbacks=callbacks,
                            last_callback_time=last_callback_time,
                            # this level always raises, input raise_on_failure
                            # processed below.
                            raise_on_failure=True,
                        )
                    except (ChainRevokedException, ChainInterruptedException) as e:
                        failures.append(e)
        if (
            failures
            and (
                raise_on_failure
                or any(
                    # historically ChainRevokedException are not swallowed by
                    # raise_exception_on_failure=True
                    isinstance(e, ChainRevokedException) for e in failures
                )
            )
        ):
            if len(failures) == 1:
                raise failures[0]
            elif failures:
                raise MultipleFailuresException(
                    task_ids=tuple(
                        str(getattr(e, 'task_id'))
                        for e in failures if hasattr(e, 'task_id')
                    ),
                    failures=tuple(failures),
                )
        return self

    def wait_for_running(self, max_wait: int=2*60) -> bool:
        sleep_between_iterations = _SLEEP_BETWEEN_ITERATIONS
        max_sleep = _SLEEP_BETWEEN_ITERATIONS * 60  # Somewhat arbitrary

        running_async_results = self._get_running()
        start_time = last_debug_output = time.monotonic()
        while running_async_results:
            time_now = time.monotonic()
            if time_now - last_debug_output >= 30:
                logger.debug(f'Waiting for running task(s): {running_async_results}')
                last_debug_output = time_now

            if (time_now - start_time) >= max_wait:
                break

            sleep_between_iterations = _sleep_exponential_backoff(
                sleep_between_iterations,
                max_sleep)
            running_async_results = running_async_results._get_running()

        if running_async_results:
            logger.error(
                f'The following tasks may still be running after task-wait timeout has expired:\n'
                f'{running_async_results}')
            return False
        return True


def _was_queue_ready(app: Celery, queue_name: str):
    return app.backend.client.sismember('QUEUES', queue_name)

S = TypeVar('S')


def create_unsuccessful_result(
    failures: Iterable[S],
    did_not_run: Iterable[S],
) -> dict[str, list[S]]:
    res = {}
    if failures_list := list(failures):
        res['failed'] = failures_list
    if did_not_run_list := list(did_not_run):
        res['not_run'] = did_not_run_list
    return res


def find_unsuccessful_in_chain(
    result: FxAsyncResult,
) -> dict[str, list[FxAsyncResult]]:
    failures : list[FxAsyncResult] = []
    did_not_run : list[FxAsyncResult] = []
    for chain_ar in result.get_chain_ancestors():
        if ( state := chain_ar.fx_get_state() ) == SUCCESS:
            pass # only reporting unsuccessful
        elif state == FAILURE:
            failures.append(chain_ar)
        else:
            # catchall
            did_not_run.append(chain_ar)

    # Should reverse the items since we're traversing the chain from RTL
    return create_unsuccessful_result(
        reversed(failures),
        reversed(did_not_run),
    )


def _check_for_failure_in_parents(result: FxAsyncResult):
    failed_ancestor : Optional[FxAsyncResult] = None
    ancestors = result.get_chain_ancestors()
    next(ancestors, None)  # get_chain_ancestors() yields result itself first; we only want its parents here.
    for ancestor in ancestors:
        if ancestor.fx_backend_get_name() is not None:
            ancestor_state = ancestor.fx_get_state()
            if ancestor_state == FAILURE:
                failed_ancestor = ancestor
                # continue in case we find a failed ancestor higher
                # in the chain, since that's what we want to report.

            if (
                failed_ancestor is None
                and (
                    ancestor_state == REVOKED
                    or RevokedRequests.is_revoked_uuid(ancestor.id)
                )
            ):
                raise ChainRevokedException(
                    task_id=ancestor.id,
                    task_name=ancestor.fx_backend_get_name(),
                )

    if failed_ancestor:
        raise _chain_interrupted_ex(failed_ancestor)


def _chain_interrupted_ex(ar: FxAsyncResult):
    return ChainInterruptedException(
        task_id=ar.id,
        task_name=ar.fx_logging_name(),
        cause=ar.fx_exception_result(),
    )


def _is_worker_alive(result: FxAsyncResult) -> bool:
    retries = 1
    tries = 0

    # NOTE: Retries for possible false negative in the case where task changes host in the small timing window
    # between getting task state / info and checking for aliveness. Retries for broker issues are handled downstream
    while tries <= retries:
        if not ( state := result.fx_get_state() ):
            logger.debug(f'Cannot get state for {result.fx_logging_name()}; assuming task is alive')
            return True

        if state in [STARTED, RECEIVED]:
            if state == RECEIVED and not result.fx_seen_queue():
                return True

            # Query the worker to see if it knows about this task
            if not ( hostname := result.fx_get_hostname() ):
                logger.debug(f'Cannot get run info for {result.fx_logging_name()}; assuming task is alive. hostname: {hostname}')
                return True

            task_info : dict[str, list[dict[str, Any]]] = fx_inspect.get_task(
                celery_app=result.app,
                method_args=(result.id,),
                destination=(hostname,),
                timeout=180) or {}
            if any(task_info.values()):
                return True

            # Try get_active and get_reserved, since we suspect query_task (the api used by get_task above)
            # may be broken sometimes.
            active_tasks_by_dest : dict[str, list[dict[str, Any]]] = fx_inspect.get_active(
                celery_app=result.app,
                destination=(hostname,),
                timeout=180) or {}
            if any(
                t.get('id') == result.id
                for t in active_tasks_by_dest.get(hostname) or []
            ):
                return True

            reserved_tasks_by_dest : dict[str, list[dict[str, str]]] = fx_inspect.get_reserved(
                celery_app=result.app,
                destination=(hostname,),
                timeout=180) or {}
            if any(
                t.get('id') == result.id
                for t in reserved_tasks_by_dest.get(hostname) or []
            ):
                return True

            logger.debug(
                f'Task inspection for {result.fx_logging_name()} on {hostname} with id '
                f'of {result.id} returned:\n{pformat(task_info)}\n'
                f'Active tasks:\n{pformat(active_tasks_by_dest)}\n'
                f'Reserved tasks:\n{pformat(reserved_tasks_by_dest)}')

        elif state == PENDING or state == RETRY:
            # Check if task queue is alive
            if not (
                (task_queue := result.fx_get_queue())
                and result.fx_seen_queue()
            ):
                return True

            queues_by_dest : dict[str, list[dict[str, str]]] = fx_inspect.get_active_queues(
                celery_app=result.app,
                timeout=180) or {}
            active_queues : set[str] = {
                q['name'] for queues in queues_by_dest.values() for q in queues
            }
            if task_queue in active_queues:
                return True

            logger.debug(f'Active queues inspection for {result.fx_logging_name()} on queue {task_queue} returned:\n'
                         f'{pformat(queues_by_dest)}\n'
                         f'Active queues: {pformat(active_queues)}')

        elif state == SUCCESS:
            return True  # Timing; possible if task state changed after we waited on it but before we got here
        else:
            logger.debug(f'Unknown state ({state} for task {result.fx_logging_name()}; assuming task is alive.')
            return True

        tries += 1
        logger.info(f'Task {result.fx_logging_name()} is not responding to queries. Tries: {tries}')

    return False


def _poll_for_ar_complete(
    result: FxAsyncResult,
    start_time: float,
    max_wait: Optional[float],
    max_sleep: float,
    callbacks: Iterable[WaitLoopCallBack],
    last_callback_time: dict[Callable, float],
):
    task_worker_failures = 0
    fail_on_worker_failures = 3
    sleep_between_iterations = _SLEEP_BETWEEN_ITERATIONS
    last_dead_task_worker_check = time.monotonic()
    while not result.fx_is_ready():
        _check_for_failure_in_parents(result)

        current_time = time.monotonic()
        if max_wait and (current_time - start_time) > max_wait:
            raise WaitOnChainTimeoutError(
                f'Result ID {result.fx_logging_name()} was not ready in {max_wait} seconds'
            )

        # callbacks
        for callback in callbacks:
            if (current_time - last_callback_time[callback.func]) > callback.frequency:
                callback.func(**callback.kwargs)
                last_callback_time[callback.func] = current_time

        # Check for dead workers
        if (current_time - last_dead_task_worker_check) > _CHECK_TASK_WORKER_FREQ:
            last_dead_task_worker_check = current_time
            if not _is_worker_alive(result):
                task_worker_failures += 1
                logger.warning(
                    f'Task {result.fx_logging_name()} appears to be a zombie.'
                    f'Failures: {task_worker_failures}',
                )
                if task_worker_failures >= fail_on_worker_failures:
                    raise ChainInterruptedByZombieTaskException(
                        task_id=result.id,
                        task_name=result.fx_get_name(),
                    )
            else:
                task_worker_failures = 0

        sleep_between_iterations = _sleep_exponential_backoff(
            sleep_between_iterations,
            max_sleep)


def _sleep_exponential_backoff(
    sleep_between_iterations: float,
    max_sleep: float,
) -> float:
    time.sleep(sleep_between_iterations)
    # Exponential backoff
    if sleep_between_iterations*1.01 < max_sleep:
        return sleep_between_iterations * 1.01
    else:
        return max_sleep


def wait_on_async_results(
    # FIXME: crazy type sig
    results: Union[FxAsyncResult, list[FxAsyncResult], None],
    max_wait: Optional[float]=None,
    callbacks: Iterable[WaitLoopCallBack] = tuple(),
    log_msg: bool=True,
    raise_exception_on_failure: bool=True,
    **_kwargs,
):
    if _kwargs:
        logger.warning(f"unexpected args: {_kwargs}")

    ManyFxAsyncResults.create_fx_ars(
        results
    ).wait_for_all(
        max_wait=max_wait,
        callbacks=callbacks,
        log_msg=log_msg,
        raise_on_failure=raise_exception_on_failure,
    )


def _warn_on_never_callback(callbacks, poll_max_wait):
    if callbacks:
        for will_not_run_callback in [c for c in callbacks if c.frequency > poll_max_wait]:
            logger.warning(f'Will not run {will_not_run_callback.func} due to frequency '
                           'being too high relative to any child poll rate.')


class WaitOnChainTimeoutError(Exception):
    pass


class ChainException(Exception):
    pass


class ChainRevokedException(ChainException):
    MESSAGE = "The chain has been interrupted by the revocation of microservice "

    def __init__(self, task_id=None, task_name=None):
        self.task_id = task_id
        self.task_name = task_name
        super(ChainRevokedException, self).__init__(task_id, task_name)

    def __str__(self):
        message = self.MESSAGE
        if self.task_name:
            message += self.task_name
        if self.task_id:
            message += '[%s]' % self.task_id
        return message


class ChainRevokedPreRunException(ChainRevokedException):
    pass


class ChainInterruptedException(ChainException):
    MESSAGE = "The chain has been interrupted by a failure in microservice "

    def __init__(self, task_id=None, task_name=None, cause=None):
        self.task_id = task_id
        self.task_name = task_name
        self.__cause__ = cause
        super(ChainInterruptedException, self).__init__(task_id, task_name, cause)

    def __str__(self):
        message = self.MESSAGE
        if self.task_name:
            message += self.task_name
        if self.task_id:
            message += '[%s]' % self.task_id
        return message


class ChainInterruptedByZombieTaskException(ChainInterruptedException):
    def __str__(self):
        return super().__str__() + ': (zombie task)'


class MultipleFailuresException(ChainInterruptedException):
    MESSAGE = "The chain has been interrupted by multiple failing microservices: %s"

    def __init__(
        self,
        task_ids: tuple[str, ...]=('UNKNOWN',),
        failures: tuple[Exception, ...]=tuple(),
    ):
        self.task_ids = task_ids
        self.failures = failures
        super(ChainInterruptedException, self).__init__()

    def __str__(self):
        return self.MESSAGE % ','.join(self.task_ids)


def _get_task_results(results: dict) -> dict:
    try:
        return_keys = results[RETURN_KEYS_KEY]
    except KeyError:
        return {}
    else:
        return {
            k: results[k]
            for k in return_keys
            if k in results} if return_keys else {}


def _get_tasks_inputs_from_result(results: dict) -> dict:
    # Returns a dict of key-value pairs of inputs passed down in the async result object
    try:
        return_keys = list(results[RETURN_KEYS_KEY])
    except KeyError:
        return results
    else:
        return_keys.append(RETURN_KEYS_KEY)
        return {
            k: v
            for k, v in results.items()
            if k not in return_keys
        }


def _get_all_results(
    result: FxAsyncResult,
    all_results: dict, # MUTATES!!!!
    return_keys_only=True,
    merge_children_results=False,
    exclude_id=None,
):

    if not result:
        return  # <-- Nothing to do

    if result.fx_is_successful():
        ret = getattr(result, 'result', {}) or {}
    else:
        ret = {}

    if not return_keys_only and ret:
        # Inputs from child win, below
        all_results.update(
            _get_tasks_inputs_from_result(ret)
        )

    children = getattr(result, 'children', []) or [] if merge_children_results else []

    for child in children:
        if exclude_id and child and child.id == exclude_id:
            continue
        # Beware, recursion
        _get_all_results(
            child,
            all_results=all_results,
            return_keys_only=return_keys_only,
            merge_children_results=merge_children_results,
            # Unnecessary; exclude_id is usually a first-level child
            exclude_id=exclude_id)

    if ret:
        # Returns from the parent win
        all_results.update(_get_task_results(ret))


def _results2tuple(
    results: dict[str, Any],
    return_keys: Union[str, Sequence[str]],
) -> tuple[Any, ...]:
    if isinstance(return_keys, str):
        return_keys = tuple([return_keys])
    results_to_return : list[Any] = []
    for key in return_keys:
        if key == DYNAMIC_RETURN:
            results_to_return.append(results)
        else:
            results_to_return.append(results.get(key))
    return tuple(results_to_return)


def _get_results_dict(
    result: FxAsyncResult,
    parent_id: Optional[str]=None,
    return_keys_only=True,
    merge_children_results=False,
    extract_from_parents=True,
) -> dict[str, Any]:
    all_results : dict[str, Any] = {}

    if extract_from_parents:
        chain_members = list(
            result.get_chain_ancestors(max_parent_id=parent_id)
        )
        while len(chain_members) > 1:
            # This means we have at least one parent to walk. Parents need to be walked first
            # because we want the latter services in a chain to override the earlier services
            # results. But we don't want to walk the child which is a member of the chain,
            # since this will be walked explicitly, so we exclude that.
            _get_all_results(
                result=chain_members.pop(),
                all_results=all_results,
                return_keys_only=return_keys_only,
                merge_children_results=merge_children_results,
                exclude_id=chain_members[-1].id,
            )

    # After possibly walking parents, we get our results for "result" (and possibly all children)
    _get_all_results(
        result=result,
        all_results=all_results,
        return_keys_only=return_keys_only,
        merge_children_results=merge_children_results,
    )

    from firexkit.bag_of_goodies import AutoInjectRegistry
    all_results.pop(AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY, None)
    return all_results


def _is_dict_results_return(return_keys: Sequence[str]) -> bool:
    return bool(
        not return_keys
        or return_keys == DYNAMIC_RETURN
        or return_keys == (DYNAMIC_RETURN,)
    )


def _get_results_tuple(
    result: FxAsyncResult,
    return_keys: Sequence[str],
    parent_id: Optional[str]=None,
    return_keys_only=True,
    merge_children_results=False,
    extract_from_parents=True,
) -> tuple[Any, ...]:
    assert return_keys
    assert not _is_dict_results_return(return_keys)
    all_results = _get_results_dict(
        result=result,
        parent_id=parent_id,
        return_keys_only=return_keys_only,
        merge_children_results=merge_children_results,
        extract_from_parents=extract_from_parents,
    )

    return _results2tuple(all_results, tuple(return_keys))


def get_results(
    result: FxAsyncResult,
    return_keys: Union[str, Sequence[str]]=tuple(),
    parent_id: Optional[str]=None,
    return_keys_only=True,
    merge_children_results=False,
    extract_from_parents=True,
) -> dict[str, Any]:
    """
    Extract and return task results

    Args:
        result: The AsyncResult to extract actual returned results from
        return_keys: A single return key string, or a tuple of keys to extract from the AsyncResult.
            The default value of :const:`None` will return a dictionary of key/value pairs for the returned results.
        return_keys_only: If :const:`True` (default), only return results for keys specified by the task's
            `@returns` decorator or :attr:`returns` attribute. If :const:`False`, returns will include key/value pairs
            from the `bag of goodies`.
        parent_id: If :attr:`extract_from_parents` is set, extract results up to this parent_id, or until we can no
            longer traverse up the parent hierarchy
        merge_children_results: If :const:`True`, traverse children of `result`, and merge results produced by them.
            The default value of :const:`False` will not collect results from the children.
        extract_from_parents: If :const:`True` (default), will consider all results returned from tasks of the given
            chain (parents of the last task). Else will consider only results returned by the last task of the chain.

    Returns:
        If `return_keys` parameter was specified, returns a tuple of the results in the same order of the return_keys.
        If `return_keys` parameter wasn't specified, return a dictionary of the key/value pairs of the returned results.
    """
    all_results = _get_results_dict(
        result=result,
        parent_id=parent_id,
        return_keys_only=return_keys_only,
        merge_children_results=merge_children_results,
        extract_from_parents=extract_from_parents,
    )
    if _is_dict_results_return(return_keys):
        return all_results
    else:
        return _results2tuple(all_results, return_keys)


def get_results_with_default(
    result: AsyncResult,
    default=None,
    error_msg: Optional[str]=None,
    **kwargs,
):
    if result.successful():
        return get_results(result, **kwargs)
    else:
        if isinstance(getattr(result, 'result'), Exception):
            exc_info = result.result
        else:
            exc_info = None
        error_msg = error_msg or f'Unable to get result from {result}'
        logger.error(error_msg, exc_info=exc_info)
        return default


#
# Returns the first exception that is not a "ChainInterruptedException"
# in the exceptions stack.
#
def first_non_chain_interrupted_exception(ex: BaseException) -> BaseException:
    e = ex
    while (
        e
        and e.__cause__ is not None
        and isinstance(e, ChainInterruptedException)
    ):
        e = e.__cause__
    return e


#
# Returns the last exception in the cause chain that is a "ChainInterruptedException"
#
def last_causing_chain_interrupted_exception(ex):
    e = ex
    while e.__cause__ is not None and isinstance(e.__cause__, ChainInterruptedException):
        e = e.__cause__
    return e


def _get_all_descendants(
    ar: FxAsyncResult,
    skip_subtree_nodes: set[str],
) -> set[FxAsyncResult]:
    stack = deque([ar])
    result_ars = set()
    while stack:
        ar = stack.popleft()
        if ar.id not in skip_subtree_nodes:
            # we waited already for readiness, so just add them here.
            result_ars.add(ar)
            stack.extend([ar for ar in (ar.children or [])])
    return result_ars


def _forget_subtree_results(
    head_node_result: FxAsyncResult,
    skip_subtree_nodes: set[str],
    do_not_forget_nodes: set[str],
):
    """
    Forget results of the subtree rooted at head_node_result, while skipping subtrees in skip_subtree_nodes,
    as well as nodes in do_not_forget_nodes
    """

    # Must get all the elements from the _get_all_descendants() generator first!
    # We can't process the forgetting of one element at a time per iteration because the parent/children relationship
    # might be lost once we forget a node
    #
    subtree_ars = _get_all_descendants(head_node_result, skip_subtree_nodes)
    nodes_to_forget = {n for n in subtree_ars if n.id not in do_not_forget_nodes}
    logger.debug(
        f'Forgetting {len(nodes_to_forget)} results for tree root {head_node_result.fx_logging_name()}'
    )
    for ar in nodes_to_forget:
        ar.fx_forget()


def forget_chain_results(
    result: FxAsyncResult,
    do_not_forget_nodes: Optional[Iterable[str]],
    skip_subtree_nodes: Optional[Iterable[str]],
):
    """
    Forget results of the tree rooted at the "chain-head" of result, while skipping subtrees in skip_subtree_nodes,
    as well as nodes in do_not_forget_nodes.
    """
    try:
        result.fx_wait(max_wait=120, raise_on_failure=False)
    except WaitOnChainTimeoutError:
        logger.error(f'Timed out waiting for {result.fx_logging_name()} to complete, will not forget.')
    else:
        for ar in result.get_chain_ancestors():
            _forget_subtree_results(
                head_node_result=ar,
                do_not_forget_nodes=set(do_not_forget_nodes or []),
                skip_subtree_nodes=set(skip_subtree_nodes or []),
            )
