import dataclasses
import datetime
import enum
import json
import os
from contextlib import contextmanager
from getpass import getuser
from socket import gethostname
from tempfile import NamedTemporaryFile
from typing import Any, TypeVar

import celery.exceptions
import psutil
from celery import bootsteps
from celery.states import RETRY, REVOKED
from celery.utils.log import get_task_logger
from typing_extensions import Self

from firexapp.common import create_link, silent_mkdir, wait_until
from firexapp.events.model import RevokeDetails
from firexapp.submit.uid import FIREX_ID_REGEX, Uid
from firexkit.result import (
    RUN_RESULTS_NAME,
    RUN_UNSUCCESSFUL_NAME,
    FxAsyncResult,
    create_unsuccessful_result,
    get_results,
)
from firexkit.task import convert_to_serializable

logger = get_task_logger(__name__)

T = TypeVar("T", bound="FireXRunData")


def _chain_to_list(chain) -> list[str]:
    # Coerce a chain in to a list of (possibly qualified) task names without
    # consulting the app's task registry.
    if isinstance(chain, str):
        chain = chain.split(",")
    return [str(s).strip() for s in chain]


def _norm_chain_names(fx_app, chain) -> list[str]:
    try:
        return [t.short_name for t in fx_app.get_app_tasks(chain)]
    except celery.exceptions.NotRegistered:
        return [s.split(".")[-1] for s in _chain_to_list(chain)]


@dataclasses.dataclass
class FireXRunData:
    firex_id: str
    logs_path: str
    completed: bool
    chain: list[str]
    submission_host: str
    submission_dir: str
    submission_cmd: list[str]
    viewers: dict[str, str]
    inputs: dict[str, Any]
    results: dict[str, Any] | None = None
    revoked: bool = False
    revoked_details: RevokeDetails | None = None
    completed_timestamp: datetime.datetime | None = None
    submit_proc_start_timestamp: datetime.datetime | None = None
    # The run's total time budget in seconds, as it currently stands. Only written once a
    # task has raised it at runtime (FireXTask.ensure_run_time_remaining); None means the
    # run is still on the budget it was submitted with, i.e. inputs['soft_time_limit'].
    # Recorded here, and not only in the broker, because the things that decide whether a
    # run has overrun -- kill_runs above all -- outlive that run's broker.
    run_soft_time_limit: float | None = None

    _extra_fields: dict[str, Any] = dataclasses.field(default_factory=dict)

    @staticmethod
    def create_from_common_run_data(
        uid: Uid,
        chain,
        submission_dir,
        argv,
        original_cli,
        inputs: dict[str, Any],
        submit_proc_start_timestamp: datetime.datetime | None = None,
    ) -> "FireXRunData":
        from firexkit.firex_celery import FireXCelery

        fx_app = FireXCelery.app_or_default()
        if chain:
            # Deliberately not normalized against the app's task registry: this runs
            # before import_microservices, so querying the registry here would finalize
            # the app before the task modules have been imported. The chain is
            # normalized later, by write_update_input_args.
            chain = _chain_to_list(chain)

        viewers = uid.viewers or {}
        _extra_fields = dict(viewers)  # backwards compat

        return FireXRunData(
            firex_id=uid.identifier,
            logs_path=uid.logs_dir,
            completed=False,
            chain=chain,
            submission_host=fx_app.conf.mc or gethostname(),
            submission_dir=submission_dir,
            submission_cmd=original_cli or list(argv or []),
            viewers=viewers,
            inputs=inputs,
            _extra_fields=_extra_fields,
            submit_proc_start_timestamp=submit_proc_start_timestamp,
        )

    @classmethod
    def run_logs_dir_from_firex_id(cls, firex_id: str) -> str:
        raise NotImplementedError()

    @classmethod
    def _create_from_dict(
        cls,
        run_dict: dict[str, Any],
    ) -> Self:
        field_names = {
            f.name for f in dataclasses.fields(cls) if f.name not in ["_extra_fields"]
        }
        modelled_fields = {k: v for k, v in run_dict.items() if k in field_names}
        extra_fields = {k: v for k, v in run_dict.items() if k not in field_names}

        #
        # special field transforms.
        #
        if modelled_fields.get("completed_timestamp"):
            modelled_fields["completed_timestamp"] = datetime.datetime.fromisoformat(
                modelled_fields["completed_timestamp"]
            )
        if modelled_fields.get("revoked_details"):
            modelled_fields["revoked_details"] = RevokeDetails(
                **modelled_fields["revoked_details"]
            )
        return cls(
            _extra_fields=extra_fields,
            **modelled_fields,
        )

    @classmethod
    def _get_completion_run_json_path(
        cls,
        logs_dir: str | None = None,
        firex_id: str | None = None,
    ) -> str:
        return os.path.join(
            cls._logs_dir_maybe_from_firex_id(logs_dir, firex_id),
            FireXJsonReportGenerator.reporter_dirname,
            FireXJsonReportGenerator.completion_report_filename,
        )

    @classmethod
    def load_run_json_file(
        cls,
        json_filepath: str,
    ) -> Self:
        with open(json_filepath, encoding="utf-8") as f:
            return cls._create_from_dict(
                json.load(fp=f),
            )

    @classmethod
    def load_initial(cls, logs_dir: str) -> Self:
        return cls.load_run_json_file(
            _get_initial_run_json_path(logs_dir),
        )

    @classmethod
    def load_from_logs_dir(cls, logs_dir: str) -> Self:
        try:
            return cls.load_run_json_file(
                cls._get_completion_run_json_path(logs_dir),
            )
        except FileNotFoundError:
            try:
                return cls.load_initial(logs_dir)
            except FileNotFoundError:
                return cls.load_run_json_file(
                    # This should be impossible but we have test data
                    # for this impossible case :/
                    _run_json_link_path_from_logs_dir(logs_dir),
                )

    @classmethod
    def load_from_firex_id(cls, firex_id: str) -> Self:
        logs_dir = cls.run_logs_dir_from_firex_id(firex_id)
        return cls.load_from_logs_dir(logs_dir)

    def write_initial_run_json(self) -> str:
        init_json_filepath = _get_initial_run_json_path(self.logs_path)
        _write_run_json(self, init_json_filepath)

        report_link = _run_json_link_path_from_logs_dir(self.logs_path)
        try:
            create_link(init_json_filepath, report_link, delete_link=False)
        except FileExistsError:
            logger.debug(
                f"f{report_link} link already exist. "
                f"No need to link to f{init_json_filepath}"
            )

        return report_link

    def write_update_input_args(self, inputs: dict[str, Any]):
        self.inputs = {
            k: v
            for k, v in inputs.items()
            if k
            not in [
                "uid",
                "chain",
                "submission_dir",
                "argv",
                "original_cli",
                "json_file",
            ]
        }
        if self.chain:
            # The app is initialized (import_microservices has run) by the time this is
            # called, so the chain names can now be resolved against the task registry.
            try:
                from firexkit.firex_celery import FireXCelery

                self.chain = _norm_chain_names(FireXCelery.app_or_default(), self.chain)
            # Updating the report is best effort and must not fail the run.
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Failed to normalize chain names: {e}")
        try:
            self._refresh_run_soft_time_limit()
            _write_run_json(self, _get_initial_run_json_path(self.logs_path))
        # Updating the report is best effort and must not fail the run.
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to update run.json with input args: {e}")

    def _refresh_run_soft_time_limit(self):
        """
        Adopts a budget raise that a worker recorded in run.json.

        The submit process holds this object for the life of the run, so its view of
        the budget goes stale the moment a task raises it; both of the writes this
        process makes would otherwise put the submitted budget back.

        No lock: run.json is only ever replaced atomically, so a reader sees one whole
        version or another, never a partial file.
        """
        try:
            persisted = self.load_initial(self.logs_path).run_soft_time_limit
        except (OSError, ValueError) as e:
            logger.warning(f"Failed reading the recorded run_soft_time_limit: {e}")
            return

        if persisted is not None and (
            self.run_soft_time_limit is None or persisted > self.run_soft_time_limit
        ):
            self.run_soft_time_limit = persisted

    @classmethod
    def persist_run_soft_time_limit(
        cls,
        logs_dir: str,
        run_soft_time_limit: float,
    ) -> float:
        """
        Records a raised run time budget in run.json; returns the value now recorded.

        Monotonic, mirroring the broker-side budget this shadows, so raises arriving
        from different tasks converge on the largest rather than on the last writer.
        The whole read-modify-write is under a lock because the requests come from
        arbitrary worker hosts -- the atomic replace in _write_run_json makes each
        write indivisible, but does nothing for two overlapping read-modify-writes.
        """
        with _run_json_lock(logs_dir):
            run_data = cls.load_initial(logs_dir)
            recorded = run_data.run_soft_time_limit
            if recorded is not None and recorded >= run_soft_time_limit:
                return recorded

            run_data.run_soft_time_limit = run_soft_time_limit
            _write_run_json(run_data, _get_initial_run_json_path(logs_dir))
            return run_soft_time_limit

    def get_results(self) -> dict[str, Any]:
        return (self.results or {}).get(RUN_RESULTS_NAME, {})

    def get_result(self, result_key, default=None):
        return self.get_results().get(result_key, default)

    def get_input(self, input_key: str, default=None):
        return self.inputs.get(input_key, default)

    def write_run_completed(
        self,
        results: dict[str, Any] | None = None,
        revoked: None | bool | str = None,
        root_task_uuid: str | None = None,
    ) -> str:
        self.completed = True
        if results is not None:
            self.results = convert_to_serializable(results)

        now = None
        if self.completed_timestamp is None:
            now = datetime.datetime.now(datetime.timezone.utc)
            self.completed_timestamp = now

        if revoked is not None:
            if isinstance(revoked, bool):
                is_revoked = revoked
                revoked_reason = "Run revoked (cancelled)"
            else:
                assert isinstance(revoked, str), f"Bad revoked: {type(revoked)}"
                is_revoked = True
                revoked_reason = revoked

            self.revoked = is_revoked
            if self.revoked:
                self.revoked_details = _get_completed_revoke_details(
                    self.logs_path,
                    revoked_reason,
                    root_task_uuid,
                    now,
                )

        # Readers prefer the completion report over the initial one, so a raise recorded
        # by a worker has to be carried across or it disappears the moment the run ends --
        # exactly when kill_runs starts asking whether the run overran.
        self._refresh_run_soft_time_limit()

        completed_json_filepath = self._get_completion_run_json_path(self.logs_path)
        _write_run_json(self, completed_json_filepath)

        report_link = _run_json_link_path_from_logs_dir(self.logs_path)
        create_link(completed_json_filepath, report_link, relative=True)
        return report_link

    def as_serializable(self) -> dict[str, Any]:
        return convert_to_serializable(
            {
                f.name: getattr(self, f.name)
                for f in dataclasses.fields(self)
                if f.name != "_extra_fields"
            }
            | self._extra_fields
        )

    def chain_results(self) -> dict[str, Any]:
        assert self.results, "Results not set; consider waiting for results."
        return self.results[RUN_RESULTS_NAME]

    def get_failed_submitted_services(self) -> list[str]:
        assert self.results, "Check for results before requesting failed services"
        # note these are just submitted, i.e. --chain,
        # services, not all failed services for the whole run.
        return (self.results.get(RUN_UNSUCCESSFUL_NAME) or {}).get("failed") or []

    def get_status_and_description(self) -> tuple["FireXRunStatus", str]:
        if not self.completed:
            return (FireXRunStatus.RUNNING, "Run is still in progress.")

        if self.revoked:
            if self.revoked_details:
                revoked_description = self.revoked_details.get_description()
            else:
                revoked_description = "Run cancelled without detailed reason."
            return FireXRunStatus.REVOKED, revoked_description

        assert self.results, (
            f"Expected completed, not-revoked run to have results, but none found. Check {self.logs_path}."
        )

        failed_services = self.get_failed_submitted_services()
        if failed_services:
            return FireXRunStatus.SOME_FAILED, ", ".join(failed_services)

        return FireXRunStatus.SUCCESS, "Submitted services completed successfully."

    def get_status(self) -> "FireXRunStatus":
        return self.get_status_and_description()[0]

    def chain_has_service(
        self,
        query_services: str | list[str],
    ) -> bool:
        if self.chain is None:
            logger.debug(f"Run {self.firex_id} has no chain")
            return False
        services = (
            [query_services] if isinstance(query_services, str) else query_services
        )
        lower_chain = [s.lower() for s in self.chain]
        return any(
            # just get the chain basename, not fully-qualified name
            query_s.split(".")[-1].lower() in lower_chain
            for query_s in services
        )

    @classmethod
    def _logs_dir_maybe_from_firex_id(
        cls,
        logs_dir: str | None = None,
        firex_id: str | None = None,
    ) -> str:
        if not logs_dir:
            assert firex_id, "Must supply logs_dir or firex_id"
            return cls.run_logs_dir_from_firex_id(firex_id)
        return logs_dir

    @classmethod
    def is_run_json_complete(
        cls,
        logs_dir: str | None = None,
        firex_id: str | None = None,
        run_json_path: str | None = None,
    ) -> bool:
        if run_json_path is not None:
            real_basename = os.path.basename(os.path.realpath(run_json_path))
            if real_basename == FireXJsonReportGenerator.completion_report_filename:
                return True

            if not os.path.islink(run_json_path) and not logs_dir and not firex_id:
                # need to read the file if it's not a symlink that will change to completion_report_filename
                try:
                    return cls.load_run_json_file(run_json_path).completed
                except OSError as e:
                    logger.warning(
                        f"Failed to read {run_json_path} while checking completeness: {e}"
                    )
                    return False

        return os.path.exists(
            cls._get_completion_run_json_path(
                logs_dir,
                firex_id,
            )
        )

    @classmethod
    def set_revoked_if_incomplete(
        cls,
        logs_dir: str | None = None,
        firex_id: str | None = None,
        shutdown_reason: str | None = None,
    ):
        try:
            logs_dir = cls._logs_dir_maybe_from_firex_id(logs_dir, firex_id)
            if not cls.is_run_json_complete(logs_dir=logs_dir):
                FireXJsonReportGenerator.create_completed_run_json(
                    logs_dir=logs_dir,
                    run_revoked=True,
                    shutdown_reason=shutdown_reason,
                )
        except OSError as e:
            logger.warning(f"Failed to maybe mark {logs_dir} complete: {e}")

    @classmethod
    def run_json_completed_time(
        cls,
        logs_dir: str | None = None,
        firex_id: str | None = None,
    ) -> datetime.datetime | None:
        logs_dir = cls._logs_dir_maybe_from_firex_id(logs_dir, firex_id)
        if cls.is_run_json_complete(logs_dir=logs_dir):
            run_data = cls.load_from_logs_dir(logs_dir)
            if run_data.completed_timestamp:
                return run_data.completed_timestamp
            else:
                completed_json = cls._get_completion_run_json_path(logs_dir=logs_dir)
                return datetime.datetime.fromtimestamp(
                    os.path.getmtime(completed_json),
                    tz=datetime.timezone.utc,
                )
        return None

    @classmethod
    def wait_for_run_json_complete(
        cls,
        logs_dir: str | None = None,
        firex_id: str | None = None,
        timeout: float = 0,
    ) -> bool:
        return wait_until(
            cls.is_run_json_complete,
            logs_dir=logs_dir,
            firex_id=firex_id,
            timeout=timeout,
            sleep_for=0.5,
        )

    def wait(self, timeout: float) -> bool:
        return self.wait_for_run_json_complete(
            logs_dir=self.logs_path,
            timeout=timeout,
        )

    def reload(self) -> Self:
        return self.load_from_logs_dir(self.logs_path)

    def get_proc_duration(self) -> float | None:
        if self.submit_proc_start_timestamp:
            if self.completed_timestamp:
                end_time = self.completed_timestamp
            elif not self.completed:
                end_time = datetime.datetime.now(datetime.timezone.utc)
            else:
                end_time = None

            if end_time:
                return (end_time - self.submit_proc_start_timestamp).total_seconds()
        return None


def _get_completed_revoke_details(
    logs_dir: str,
    shutdown_revoke_reason: str,
    root_task_uuid: str | None,
    completed_timestamp: datetime.datetime | None,
) -> RevokeDetails | None:
    try:
        tracked_revoked_details = RevokeDetails.load_latest_run_revoke_details(logs_dir)
        # FIXME: should check the revoke request is recent enough to be the cause.
        if tracked_revoked_details:
            revoked_details = tracked_revoked_details
        elif shutdown_revoke_reason:
            revoked_details = RevokeDetails(
                logs_dir,
                reason=shutdown_revoke_reason,
                task_uuid=root_task_uuid or "RUN-TASK",
                root_revoke=True,
                revoking_user=getuser(),
            )
        else:
            revoked_details = None

        if revoked_details:
            revoked_details.write_revoke_complete(completed_timestamp)
        return revoked_details
    except OSError:
        logger.exception("Failed to get completed revoke request")
    return None


class FireXRunStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    SOME_FAILED = "SOME_FAILED"
    REVOKED = "REVOKED"

    def is_revoked(self) -> bool:
        return self == FireXRunStatus.REVOKED

    def is_failed(self) -> bool:
        return self == FireXRunStatus.SOME_FAILED

    def is_success(self) -> bool:
        return self == FireXRunStatus.SUCCESS

    def is_running(self) -> bool:
        return self == FireXRunStatus.RUNNING

    def is_complete(self) -> bool:
        return self != FireXRunStatus.RUNNING


def _write_run_json(data: FireXRunData, report_file: str):

    # Create the json_reporter dir if it doesn't exist
    silent_mkdir(os.path.dirname(report_file))

    # Atomic write, because the completed_run_json can be written from various places, including
    # celery poolworker which runs FireXRunner, celery mainprocess (as a last-resort backup in a bootstep),
    # and in another process (in the sync case). And although the backup method should kick in only after
    # other methods have failed, it's a theoretical possibility they will run concurrently depending
    # on the order of kill signals, especially in the sync case.
    with NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=os.path.dirname(report_file), delete=False
    ) as f:
        json.dump(data.as_serializable(), fp=f, skipkeys=True, sort_keys=True, indent=4)
        f.flush()
        os.fsync(f.fileno())

    os.chmod(f.name, 0o644)
    os.replace(f.name, report_file)


def _run_json_link_path_from_logs_dir(logs_dir) -> str:
    return os.path.join(logs_dir, "run.json")


# flufl.lock rather than fcntl: logs dirs are on NFS, where flock is unreliable, and the
# writers are on different hosts. lifetime is the break-in period if a holder dies.
_RUN_JSON_LOCK_LIFETIME = 5
_RUN_JSON_LOCK_TIMEOUT = 30


@contextmanager
def _run_json_lock(logs_dir: str):
    """Serialises read-modify-writes of a run's run.json across hosts."""
    # Imported here rather than at module scope: this module is imported by nearly
    # everything, while the lock is only needed on the rare budget-raise path.
    from flufl.lock import Lock

    reporter_dir = os.path.join(
        logs_dir,
        FireXJsonReportGenerator.reporter_dirname,
    )
    silent_mkdir(reporter_dir)
    with Lock(
        os.path.join(reporter_dir, "run_json.lock"),
        lifetime=_RUN_JSON_LOCK_LIFETIME,
        default_timeout=_RUN_JSON_LOCK_TIMEOUT,
    ):
        yield


class FireXJsonReportGenerator:
    reporter_dirname = "json_reporter"
    initial_report_filename = "initial_report.json"
    completion_report_filename = "completion_report.json"

    @staticmethod
    def create_initial_run_json(
        uid: Uid,
        chain,
        submission_dir,
        argv,
        original_cli=None,
        json_file=None,
        **inputs,
    ) -> FireXRunData:
        run_info = FireXRunData.create_from_common_run_data(
            uid,
            chain,
            submission_dir,
            argv,
            original_cli,
            inputs,
            submit_proc_start_timestamp=datetime.datetime.fromtimestamp(
                psutil.Process().create_time(), tz=datetime.timezone.utc
            ),
        )
        report_link = run_info.write_initial_run_json()
        if json_file:
            try:
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                logger.debug(
                    f"{json_file} link already exist; "
                    f"post_run must have already created the link to {report_link}"
                )

        return run_info

    @classmethod
    def create_completed_run_json(
        cls,
        uid: Uid | None = None,
        run_revoked: bool = True,
        chain=None,
        root_id=None,
        submission_dir=None,
        argv=None,
        original_cli=None,
        json_file=None,
        logs_dir: str | None = None,
        shutdown_reason: str | None = None,
        **inputs,
    ):
        if not logs_dir and uid is None:
            raise ValueError('At least one of "logs_dir" or "uid" must be supplied')
        elif uid:
            logs_path = uid.logs_dir
        else:
            assert logs_dir
            logs_path = logs_dir

        try:
            run_info = FireXRunData.load_initial(logs_path)
        except OSError:
            logger.warning(
                f"Failed to read initial json for {logs_path}. Creating a minimal completion report."
            )
            if not uid:
                raise

            # best effort -- not all termination contexts have access to all this data :/
            run_info = FireXRunData.create_from_common_run_data(
                uid,
                chain,
                submission_dir,
                argv,
                original_cli,
                inputs,
            )

        report_link = run_info.write_run_completed(
            results=_get_run_results_from_root_task_promise(
                root_id, run_revoked, shutdown_reason
            ),
            revoked=run_revoked and shutdown_reason,
            root_task_uuid=root_id.id if root_id else None,
        )

        if json_file:
            try:
                # This is typically not required, unless post_run ran before pre_run
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                pass  # This is expected for most cases


def _get_run_results_from_root_task_promise(
    root_task_ar: FxAsyncResult | None,
    run_revoked: bool,
    shutdown_reason: str | None,
) -> dict[str, Any]:
    if root_task_ar and root_task_ar.successful():
        return get_results(root_task_ar)

    failures = []
    did_not_run = []
    if run_revoked or (root_task_ar and root_task_ar.state in [REVOKED, RETRY]):
        did_not_run = [
            f"was revoked (i.e. cancelled){f' due to: {shutdown_reason}' if shutdown_reason else ''}"
        ]
    elif root_task_ar and root_task_ar.fx_is_failed():
        failures = [f"Run failed: {root_task_ar.result}"]
    else:
        failures = [f"Run failed before starting: {shutdown_reason or ''}"]

    return {
        RUN_RESULTS_NAME: {},
        RUN_UNSUCCESSFUL_NAME: create_unsuccessful_result(failures, did_not_run),
    }


def _get_initial_run_json_path(logs_dir):
    return os.path.join(
        logs_dir,
        FireXJsonReportGenerator.reporter_dirname,
        FireXJsonReportGenerator.initial_report_filename,
    )


class ReporterStep(bootsteps.StartStopStep):
    def include_if(self, parent):
        return parent.hostname.startswith(parent.app.conf.primary_worker_name + "@")

    def __init__(self, parent, **kwargs):
        self._logs_dir = None
        logfile = os.path.normpath(kwargs.get("logfile", "") or "")

        while (sp := os.path.split(logfile))[0] != logfile:
            m = FIREX_ID_REGEX.search(sp[1])
            if m:
                self._logs_dir = logfile
                break
            logfile = sp[0]

        super().__init__(parent, **kwargs)

    def stop(self, parent):
        # By now, the report should have been written! Write a default completion report
        if self._logs_dir:
            FireXRunData.set_revoked_if_incomplete(
                logs_dir=self._logs_dir,
                shutdown_reason="Celery stop bootstep unexpectedly found incomplete run",
            )
