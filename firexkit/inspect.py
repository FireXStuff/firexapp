import time
from typing import Iterable, Union, Any, Optional, Sequence
from typing_extensions import Self
import socket
import psutil

from celery import Celery
from celery.local import Proxy
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def inspect_with_retry(
    inspect_retry_timeout=30,
    inspect_method=None,
    retry_if_None_returned=True,
    celery_app: Union[Celery, Proxy, None]=None,
    method_args: Optional[Iterable[Any]]=None,
    verbose=False,
    **inspect_opts,
):
    if celery_app is None:
        from celery import current_app
        celery_app = current_app

    inspect_retry_timeout = inspect_retry_timeout if inspect_retry_timeout else 0
    timeout_time = time.monotonic() + inspect_retry_timeout

    if method_args is None:
        method_args = ()

    def _get_result_summary(result):
        if result:
            return {k: len(v) for k,v in result.items()}
        else:
            return result

    def _inspect(celery_app, inspect_method, method_args, **inspect_opts):
        i = celery_app.control.inspect(**inspect_opts)
        if inspect_method:
            header = f'[inspect] app.control.inspect({inspect_opts or ""}).{inspect_method}({method_args or ""})'
            if verbose:
                logger.debug(header)

            try:
                result = getattr(i, inspect_method)(*method_args)
            except Exception as e:
                # need to handle different exceptions from different brokers
                e_name = type(e).__name__
                if e_name != "TimeoutError" and e_name != "ConnectionError":
                    raise

                result = None
                logger.debug('Connection error during broker inspection', exc_info=e)

            if verbose:
                logger.debug(f'{header} returned {_get_result_summary(result)}')

            return result
        else:
            header = f'[inspect] app.control.inspect({inspect_opts or ""})'
            if verbose:
                logger.debug(header)

            return i

    inspection_result = _inspect(celery_app, inspect_method, method_args, **inspect_opts)
    while inspection_result is None and retry_if_None_returned and time.monotonic() < timeout_time:
        time.sleep(0.1)
        logger.debug(f'[inspect] Retrying for a maximum of {inspect_retry_timeout}s')
        inspection_result = _inspect(celery_app, inspect_method, method_args, **inspect_opts)
    return inspection_result


def get_active(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='active', **kwargs)


def get_reserved(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='reserved', **kwargs)


def get_scheduled(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='scheduled', **kwargs)


def get_revoked(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='revoked', **kwargs)


def get_active_queues(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='active_queues', **kwargs)


def get_task(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='query_task', **kwargs)

def ping(**kwargs):
    kwargs.pop('inspect_method', None)
    return inspect_with_retry(inspect_method='ping', **kwargs)

import pydantic

# celery defaults to 1.0, which might not be long enough internally,
# but leave for now.
_DEFAULT_INSPECT_TIMEOUT = 1.0

class InspectedTask(pydantic.BaseModel):
    # not modelled: args, kwargs, type, delivery_info
    id: str
    name: str
    status: str # active|reserved| ?
    celery_destination: str
    hostname: Optional[str] = None # e.g. mc@sjc-ads-6971, actually fx_worker_name not host hostname
    time_start: Optional[float] = None
    worker_pid: Optional[int] = None
    acknowledged: bool = False

    _is_localhost: Optional[bool]=None

    def get_only_hostname(self) -> Optional[str]:
        if self.hostname is None:
            return None
        return self.hostname.split('@')[-1]

    def is_dead_active_localhost_proc(self) -> bool:
        if (
            not self.is_localhost()
            or self.status != 'active'
            or not self.worker_pid
        ):
            return False
        # only return True when proc is confirmed not alive
        # (i.e. None alive means dead=False)
        return self.is_alive_localhost_proc() is False

    def is_localhost(self) -> Optional[bool]:
        if self.hostname is None:
            return None # don't know yet if this task will run on localhost
        if self._is_localhost is None:
            self._is_localhost = bool(self.get_only_hostname() == socket.gethostname())
        return self._is_localhost

    def is_alive_localhost_proc(self) -> Optional[bool]: # None means "failed to confirm liveness or deadness"
        assert self.is_localhost(), f'Cannot check for liveness for task {self.id} on remote host {self.get_only_hostname()}'

        try:
            task_proc = psutil.Process(pid=self.worker_pid)
        except psutil.NoSuchProcess:
            pass
        except psutil.Error as e:
            logger.error(f'Unexpected error inspecting task {self.id} proc {self.worker_pid}: {e}')
            return None
        else:
            # TODO: full "is firex worker proc" check, e.g. pidfile in cmdline, etc.
            if ( proc_name := task_proc.name() ) == 'celery':
                return True
            else:
                logger.warning(
                    f'Found unexpected proc name for task {self.id} and pid {self.worker_pid}: {proc_name}'
                )

        return False

    @classmethod
    def _inspect_status(
        cls,
        celery_app,
        query_task_status: str,
        destinations: Optional[Sequence[str]]=None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[str, list[Self]]:
        assert query_task_status in ['active', 'reserved', 'scheduled', 'revoked']
        tasks_by_dest : dict[str, list[dict[str, Any]]] = inspect_with_retry(
            inspect_method=query_task_status,
            celery_app=celery_app,
            destination=destinations,
            timeout=timeout,
        ) or {}

        modelled_tasks_by_dest : dict[str, list[Self]] = {
            d: [] for d in (destinations or [])
        }
        for d, ts in tasks_by_dest.items():
            modelled_tasks : list[Self] = []
            for t in ts:
                try:
                    modelled_tasks.append(
                        cls.model_validate(
                            t | dict(
                                status=query_task_status,
                                celery_destination=d,
                            )
                        )
                    )
                except ValueError as e:
                    logger.error(f'Failure {e} while validating Celery task: {t}')
            modelled_tasks_by_dest[d] = modelled_tasks

        return modelled_tasks_by_dest

    @classmethod
    def inspect_active(
        cls,
        celery_app,
        destinations: Optional[Sequence[str]]=None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[str, list[Self]]:
        return cls._inspect_status(
            celery_app=celery_app,
            query_task_status='active',
            destinations=destinations,
            timeout=timeout,
        )

    @classmethod
    def inspect_active_single_destination(
        cls,
        celery_app,
        destination: str,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self]:
        return cls.inspect_active(
            celery_app,
            destinations=[destination],
            timeout=timeout,
        ).get(destination) or []

    @classmethod
    def inspect_reserved_single_destination(
        cls,
        celery_app,
        destination: str,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self]:
        return cls._inspect_status(
            celery_app=celery_app,
            query_task_status='reserved',
            destinations=[destination],
            timeout=timeout,
        ).get(destination) or []

    @classmethod
    def inspect_query_tasks(
        cls,
        celery_app,
        query_task_ids: Sequence[str],
        destinations: Optional[Sequence[str]]=None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self]:
        tasks_by_dest_and_id : dict[
            str, # celery destination, e.g. mc@sjc-ads-6971
            dict[
                str, # task uuid, e.g. '886fdfcd-50d6-4174-af9f-02e108455da2'
                tuple[
                    str, # state? e.g. active
                    dict[str, Any], # task, not sure what all possibilies are.
                ]
            ]
        ] = get_task(
            celery_app=celery_app,
            method_args=tuple(query_task_ids),
            destination=destinations,
            timeout=timeout,
        ) or {}
        if not tasks_by_dest_and_id:
            logger.warning(f'Found no tasks for {query_task_ids}{ " on " + (",".join(destinations) if destinations else "")}')

        tasks = []
        for celery_dest, status_and_tasks_by_id in tasks_by_dest_and_id.items():
            for resp_task_id, status_and_task in status_and_tasks_by_id.items():
                if resp_task_id not in query_task_ids:
                    logger.error(f'Query response unexpectedly included task id: {resp_task_id}')
                try:
                    status, task_dict = status_and_task
                    tasks.append(
                        cls.model_validate(
                            task_dict | dict(
                                status=status,
                                celery_destination=celery_dest,
                            )
                        )
                    )
                except ValueError as e:
                    logger.error(f'Failure {e} while validating Celery task: {status_and_task}')

        if missing_task_ids := set(query_task_ids) - {t.id for t in tasks}:
            logger.error(f'Failed to find task ids: {", ".join(missing_task_ids)}')

        return tasks

    @classmethod
    def inspect_query_single_task(
        cls,
        celery_app,
        query_task_id: str,
        destinations: Optional[Sequence[str]]=None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> Optional[Self]:
        tasks = cls.inspect_query_tasks(
            celery_app=celery_app,
            query_task_ids=(query_task_id,),
            destinations=destinations,
            timeout=timeout,
        )
        if not tasks:
            logger.warning(f'Found no tasks for {query_task_id}{ " on " + (",".join(destinations) if destinations else "")}')
        else:
            if len(tasks) > 1:
                logger.error(f'Unexpectedly found multiple tasks for {query_task_id}: {tasks}')
            task = next(
                (t for t in tasks if t.id == query_task_id),
                tasks[0],
            )
            if task.id != query_task_id:
                logger.error(f'Unexpectedly found task ID {task.id} when querying for {query_task_id}')
            return task

        return None
