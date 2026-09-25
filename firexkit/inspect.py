import socket
import time
from collections.abc import Iterable, Sequence
from typing import Any

import psutil
import pydantic
from celery import Celery
from celery.local import Proxy
from celery.utils.log import get_task_logger
from typing_extensions import Self

from firexkit.firex_worker import FxBuiltinQueues, FxWorkerHostName, FxWorkerName

logger = get_task_logger(__name__)


def inspect_with_retry(
    inspect_retry_timeout=30,
    inspect_method=None,
    retry_if_None_returned=True,
    celery_app: Celery | Proxy | None = None,
    method_args: Iterable[Any] | None = None,
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
            return {k: len(v) for k, v in result.items()}
        else:
            return result

    def _inspect(celery_app, inspect_method, method_args, **inspect_opts):
        i = celery_app.control.inspect(**inspect_opts)
        if inspect_method:
            header = f"[inspect] app.control.inspect({inspect_opts or ''}).{inspect_method}({method_args or ''})"
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
                logger.debug("Connection error during broker inspection", exc_info=e)

            if verbose:
                logger.debug(f"{header} returned {_get_result_summary(result)}")

            return result
        else:
            header = f"[inspect] app.control.inspect({inspect_opts or ''})"
            if verbose:
                logger.debug(header)

            return i

    inspection_result = _inspect(
        celery_app, inspect_method, method_args, **inspect_opts
    )
    while (
        inspection_result is None
        and retry_if_None_returned
        and time.monotonic() < timeout_time
    ):
        time.sleep(0.1)
        logger.debug(f"[inspect] Retrying for a maximum of {inspect_retry_timeout}s")
        inspection_result = _inspect(
            celery_app, inspect_method, method_args, **inspect_opts
        )
    return inspection_result


def get_active(**kwargs):
    kwargs.pop("inspect_method", None)
    return inspect_with_retry(inspect_method="active", **kwargs)


def get_reserved(**kwargs):
    kwargs.pop("inspect_method", None)
    return inspect_with_retry(inspect_method="reserved", **kwargs)


def get_scheduled(**kwargs):
    kwargs.pop("inspect_method", None)
    return inspect_with_retry(inspect_method="scheduled", **kwargs)


def get_revoked(**kwargs):
    kwargs.pop("inspect_method", None)
    return inspect_with_retry(inspect_method="revoked", **kwargs)


def _get_task(**kwargs):
    kwargs.pop("inspect_method", None)
    return inspect_with_retry(inspect_method="query_task", **kwargs)


def ping(**kwargs):
    kwargs.pop("inspect_method", None)
    return inspect_with_retry(inspect_method="ping", **kwargs)


# celery defaults to 1.0, which might not be long enough internally,
# but leave for now.
_DEFAULT_INSPECT_TIMEOUT = 1.0


def _as_celery_destinations(
    destinations: Sequence[str | FxWorkerName] | None,
) -> tuple[str, ...] | None:
    """Celery knows a worker only by the string its name renders to."""
    if destinations is None:
        return None
    return tuple(str(d) for d in destinations)


def _on_destinations(
    destinations: Sequence[str | FxWorkerName] | None,
) -> str:
    """Name the destinations a broadcast was addressed to, for logging."""
    if not destinations:
        return ""  # a broadcast reaches every worker, so there is nothing to name.
    return " on " + ",".join(str(d) for d in destinations)


class InspectedTask(pydantic.BaseModel):
    # not modelled: args, kwargs, type, delivery_info
    id: str
    name: str
    status: str  # active|reserved| ?
    celery_destination: str
    hostname: str | None = (
        None  # e.g. mc@sjc-ads-6971, actually fx_worker_name not host hostname
    )
    time_start: float | None = None
    worker_pid: int | None = None
    acknowledged: bool = False

    _is_localhost: bool | None = None

    def __str__(self):
        return f"{self.name}[{self.id}] (pid {self.worker_pid})"

    def get_only_hostname(self) -> str | None:
        if self.hostname is None:
            return None
        return self.hostname.split("@")[-1]

    def is_dead_active_localhost_proc(self) -> bool:
        if not self.is_localhost() or self.status != "active" or not self.worker_pid:
            return False
        # only return True when proc is confirmed not alive
        # (i.e. None alive means dead=False)
        return self.is_alive_localhost_proc() is False

    def is_localhost(self) -> bool | None:
        if self.hostname is None:
            return None  # don't know yet if this task will run on localhost
        if self._is_localhost is None:
            self._is_localhost = bool(self.get_only_hostname() == socket.gethostname())
        return self._is_localhost

    def is_alive_localhost_proc(
        self,
    ) -> bool | None:  # None means "failed to confirm liveness or deadness"
        assert self.is_localhost(), (
            f"Cannot check for liveness for task {self.id} on remote host {self.get_only_hostname()}"
        )

        try:
            task_proc = psutil.Process(pid=self.worker_pid)
        except psutil.NoSuchProcess:
            pass
        except psutil.Error as e:
            logger.error(
                f"Unexpected error inspecting task {self.id} proc {self.worker_pid}: {e}"
            )
            return None
        else:
            # TODO: full "is firex worker proc" check, e.g. pidfile in cmdline, etc.
            if (proc_name := task_proc.name()) == "celery":
                return True
            else:
                logger.warning(
                    f"Found unexpected proc name for task {self.id} and pid {self.worker_pid}: {proc_name}"
                )

        return False

    @classmethod
    def _inspect_status(
        cls,
        celery_app,
        query_task_status: str,
        destinations: Sequence[str | FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[str, list[Self]]:
        assert query_task_status in ["active", "reserved", "scheduled", "revoked"]
        tasks_by_dest: dict[str, list[dict[str, Any]]] = (
            inspect_with_retry(
                inspect_method=query_task_status,
                celery_app=celery_app,
                destination=_as_celery_destinations(destinations),
                timeout=timeout,
            )
            or {}
        )

        # Only destinations that replied are present: a requested destination
        # that didn't answer is absent, which is not the same as a destination
        # that answered with no tasks (present with an empty list).
        modelled_tasks_by_dest: dict[str, list[Self]] = {}
        for d, ts in tasks_by_dest.items():
            modelled_tasks: list[Self] = []
            for t in ts:
                try:
                    modelled_tasks.append(
                        cls.model_validate(
                            t
                            | {
                                "status": query_task_status,
                                "celery_destination": d,
                            }
                        )
                    )
                except ValueError as e:
                    logger.error(f"Failure {e} while validating Celery task: {t}")
            modelled_tasks_by_dest[d] = modelled_tasks

        return modelled_tasks_by_dest

    @classmethod
    def inspect_active(
        cls,
        celery_app,
        destinations: Sequence[str | FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[str, list[Self]]:
        return cls._inspect_status(
            celery_app=celery_app,
            query_task_status="active",
            destinations=destinations,
            timeout=timeout,
        )

    @classmethod
    def inspect_active_single_destination(
        cls,
        celery_app,
        destination: str | FxWorkerHostName,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self]:
        # Absent when the destination didn't respond, so tolerate a missing entry.
        return (
            cls.inspect_active(
                celery_app,
                destinations=[destination],
                timeout=timeout,
            ).get(str(destination))
            or []
        )

    @classmethod
    def inspect_scheduled(
        cls,
        celery_app,
        destinations: Sequence[str | FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[str, list[Self]]:
        return cls._inspect_status(
            celery_app=celery_app,
            query_task_status="scheduled",
            destinations=destinations,
            timeout=timeout,
        )

    @classmethod
    def inspect_reserved(
        cls,
        celery_app,
        destinations: Sequence[str | FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[str, list[Self]]:
        return cls._inspect_status(
            celery_app=celery_app,
            query_task_status="reserved",
            destinations=destinations,
            timeout=timeout,
        )

    @classmethod
    def inspect_reserved_single_destination(
        cls,
        celery_app,
        destination: str | FxWorkerHostName,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self]:
        # Absent when the destination didn't respond, so tolerate a missing entry.
        return (
            cls.inspect_reserved(
                celery_app=celery_app,
                destinations=[destination],
                timeout=timeout,
            ).get(str(destination))
            or []
        )

    @classmethod
    def inspect_query_tasks(
        cls,
        celery_app,
        query_task_ids: Sequence[str],
        destinations: Sequence[str | FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self]:
        tasks_by_dest_and_id: dict[
            str,  # celery destination, e.g. mc@sjc-ads-6971
            dict[
                str,  # task uuid, e.g. '886fdfcd-50d6-4174-af9f-02e108455da2'
                tuple[
                    str,  # state? e.g. active
                    dict[str, Any],  # task, not sure what all possibilies are.
                ],
            ],
        ] = (
            _get_task(
                celery_app=celery_app,
                method_args=tuple(query_task_ids),
                destination=_as_celery_destinations(destinations),
                timeout=timeout,
            )
            or {}
        )
        if not tasks_by_dest_and_id:
            logger.warning(
                f"Found no tasks for {query_task_ids}{_on_destinations(destinations)}"
            )

        tasks = []
        for celery_dest, status_and_tasks_by_id in tasks_by_dest_and_id.items():
            for resp_task_id, status_and_task in status_and_tasks_by_id.items():
                if resp_task_id not in query_task_ids:
                    logger.error(
                        f"Query response unexpectedly included task id: {resp_task_id}"
                    )
                try:
                    status, task_dict = status_and_task
                    tasks.append(
                        cls.model_validate(
                            task_dict
                            | {
                                "status": status,
                                "celery_destination": celery_dest,
                            }
                        )
                    )
                except ValueError as e:
                    logger.error(
                        f"Failure {e} while validating Celery task: {status_and_task}"
                    )

        if missing_task_ids := set(query_task_ids) - {t.id for t in tasks}:
            logger.error(f"Failed to find task ids: {', '.join(missing_task_ids)}")

        return tasks

    @classmethod
    def inspect_query_single_task(
        cls,
        celery_app,
        query_task_id: str,
        destinations: Sequence[str | FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> Self | None:
        tasks = cls.inspect_query_tasks(
            celery_app=celery_app,
            query_task_ids=(query_task_id,),
            destinations=destinations,
            timeout=timeout,
        )
        if not tasks:
            logger.warning(
                f"Found no tasks for {query_task_id}{_on_destinations(destinations)}"
            )
        else:
            if len(tasks) > 1:
                logger.error(
                    f"Unexpectedly found multiple tasks for {query_task_id}: {tasks}"
                )
            task = next(
                (t for t in tasks if t.id == query_task_id),
                tasks[0],
            )
            if task.id != query_task_id:
                logger.error(
                    f"Unexpectedly found task ID {task.id} when querying for {query_task_id}"
                )
            return task

        return None


class InspectedQueue(pydantic.BaseModel):
    """A queue a worker replied it is consuming from.

    Celery describes queues in RabbitMQ/AMQP terms, most of which mean nothing
    for the other brokers; only the fields that carry over are modelled here.
    Not modelled: exchange, bindings, binding_arguments, queue_arguments,
    consumer_arguments, expires, message_ttl, max_length, max_length_bytes,
    max_priority, no_declare.
    """

    name: str
    alias: str | None = None
    routing_key: str | None = None
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    no_ack: bool = False

    def __str__(self):
        return self.name

    def is_builtin_queue(self, fx_queue: FxBuiltinQueues) -> bool:
        """Whether this is the given builtin queue, for any spawn group or host.

        For example the shutdown queue is really a family of queues -- one per
        host and one per master on it -- that all answer to the same name.
        """
        # Queue names optionally carry a spawn group and a host, e.g.
        # shutdown:g1@a-host, in the same shape worker names do.
        queue_name = self.name.split("@")[0]
        return queue_name == str(fx_queue) or queue_name.startswith(f"{fx_queue}:")

    @classmethod
    def inspect_active_queues(
        cls,
        celery_app,
        destinations: Sequence[FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> dict[FxWorkerHostName, list[Self]]:
        """The queues each worker replied it is consuming from.

        Only destinations that replied are present: a requested destination
        that didn't answer is absent, which is not the same as a destination
        that answered with no queues (present with an empty list).
        """
        queues_by_dest: dict[str, list[dict[str, Any]]] = (
            inspect_with_retry(
                inspect_method="active_queues",
                celery_app=celery_app,
                destination=_as_celery_destinations(destinations),
                timeout=timeout,
            )
            or {}
        )

        modelled_queues_by_dest: dict[FxWorkerHostName, list[Self]] = {}
        for d, queues in queues_by_dest.items():
            try:
                worker = FxWorkerHostName.fx_worker_host_name_from_str(d)
            except (AssertionError, ValueError) as e:
                logger.error(f"Failure {e} while parsing Celery destination: {d}")
                continue

            modelled_queues: list[Self] = []
            for q in queues:
                try:
                    modelled_queues.append(cls.model_validate(q))
                except ValueError as e:
                    logger.error(f"Failure {e} while validating Celery queue: {q}")
            modelled_queues_by_dest[worker] = modelled_queues

        return modelled_queues_by_dest

    @classmethod
    def inspect_active_queues_single_destination(
        cls,
        celery_app,
        destination: FxWorkerHostName,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> list[Self] | None:
        """None when the destination didn't reply, which is not the same as it
        replying that it consumes no queues (an empty list)."""
        return cls.inspect_active_queues(
            celery_app=celery_app,
            destinations=[destination],
            timeout=timeout,
        ).get(destination)

    @classmethod
    def inspect_active_queue_names(
        cls,
        celery_app,
        destinations: Sequence[FxWorkerHostName] | None = None,
        timeout=_DEFAULT_INSPECT_TIMEOUT,
    ) -> set[str]:
        """Every queue name being consumed, no matter which worker consumes it."""
        return {
            q.name
            for queues in cls.inspect_active_queues(
                celery_app=celery_app,
                destinations=destinations,
                timeout=timeout,
            ).values()
            for q in queues
        }
