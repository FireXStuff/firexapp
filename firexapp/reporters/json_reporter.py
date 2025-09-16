import json
import os
from socket import gethostname
import dataclasses
from tempfile import NamedTemporaryFile
from typing import Union, Optional, Any, TypeVar, Type
import datetime
import enum
import pytz
from getpass import getuser
import psutil

from celery import bootsteps
from celery.worker.components import Hub
import celery.exceptions
from celery.result import AsyncResult

from firexapp.application import get_app_tasks
from firexapp.common import silent_mkdir, create_link, wait_until
from firexapp.submit.uid import FIREX_ID_REGEX, Uid
from firexkit.result import (
    create_unsuccessful_result,
    RUN_RESULTS_NAME,
    RUN_UNSUCCESSFUL_NAME,
    get_results,
)
from celery.states import REVOKED, RETRY
from firexkit.task import convert_to_serializable
from celery.utils.log import get_task_logger
from firexapp.engine.celery import app
from firexapp.engine.firex_revoke import RevokeDetails

logger = get_task_logger(__name__)

T = TypeVar('T', bound='FireXRunData')


def norm_chain_names(chain) -> list[str]:
    try:
        return [t.short_name for t in get_app_tasks(chain)]
    except celery.exceptions.NotRegistered:
        if isinstance(chain, str):
            chain = chain.split(',')
        return [s.strip().split('.')[-1] for s in chain]

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
    results: Optional[dict[str, Any]] = None
    revoked: bool = False
    revoked_details: Optional['RevokeDetails'] = None
    completed_timestamp: Optional[datetime.datetime] = None
    submit_proc_start_timestamp: Optional[datetime.datetime] = None

    _extra_fields: dict[str, Any] = dataclasses.field(default_factory=dict)

    @staticmethod
    def create_from_common_run_data(
        uid: Uid,
        chain,
        submission_dir,
        argv,
        original_cli,
        inputs: dict[str, Any],
        submit_proc_start_timestamp: Optional[datetime.datetime]=None,
    ) -> 'FireXRunData':
        if chain:
            chain = norm_chain_names(chain)

        viewers = uid.viewers or {}
        _extra_fields = dict(viewers) # backwards compat

        return FireXRunData(
            firex_id=uid.identifier,
            logs_path=uid.logs_dir,
            completed=False,
            chain=chain,
            submission_host=app.conf.mc or gethostname(),
            submission_dir=submission_dir,
            submission_cmd=original_cli or list(argv or []),
            viewers=viewers,
            inputs=inputs,
            _extra_fields=_extra_fields,
            submit_proc_start_timestamp=submit_proc_start_timestamp,
        )

    @classmethod
    def run_logs_dir_from_firex_id(cls: Type[T], firex_id: str) -> str:
        raise NotImplementedError()

    @classmethod
    def _create_from_dict(
        cls: Type[T],
        run_dict: dict[str, Any],
    ) -> T:
        field_names = {
            f.name for f in dataclasses.fields(cls)
            if f.name not in ['_extra_fields']
        }
        modelled_fields = {
            k: v for k, v in run_dict.items()
            if k in field_names
        }
        extra_fields = {
            k: v for k, v in run_dict.items()
            if k not in field_names
        }

        #
        # special field transforms.
        #
        if modelled_fields.get('completed_timestamp'):
            modelled_fields['completed_timestamp'] = datetime.datetime.fromisoformat(
                modelled_fields['completed_timestamp']
            )
        if modelled_fields.get('revoked_details'):
            modelled_fields['revoked_details'] = RevokeDetails(
                **modelled_fields['revoked_details']
            )
        return cls(
            _extra_fields=extra_fields,
            **modelled_fields,
        )

    @classmethod
    def _get_completion_run_json_path(
        cls,
        logs_dir: Optional[str]=None,
        firex_id: Optional[str]=None,
    ) -> str:
        return os.path.join(
            cls._logs_dir_maybe_from_firex_id(logs_dir, firex_id),
            FireXJsonReportGenerator.reporter_dirname,
            FireXJsonReportGenerator.completion_report_filename,
        )

    @classmethod
    def load_run_json_file(
        cls: Type[T],
        json_filepath: str,
    ) -> T:
        with open(json_filepath, encoding='utf-8') as f:
            return cls._create_from_dict(
                json.load(fp=f),
            )

    @classmethod
    def load_initial(cls: Type[T], logs_dir: str) -> T:
        return cls.load_run_json_file(
            _get_initial_run_json_path(logs_dir),
        )

    @classmethod
    def load_from_logs_dir(cls: Type[T], logs_dir: str) -> T:
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
    def load_from_firex_id(cls: Type[T], firex_id: str) -> T:
        logs_dir = cls.run_logs_dir_from_firex_id(firex_id)
        return cls.load_from_logs_dir(logs_dir)

    def write_initial_run_json(self) -> str:
        init_json_filepath = _get_initial_run_json_path(self.logs_path)
        _write_run_json(self, init_json_filepath)

        report_link = _run_json_link_path_from_logs_dir(self.logs_path)
        try:
            create_link(init_json_filepath, report_link, delete_link=False)
        except FileExistsError:
            logger.debug(f'f{report_link} link already exist. '
                         f'No need to link to f{init_json_filepath}')

        return report_link

    def write_update_input_args(self, inputs: dict[str, Any]):
        self.inputs = {
            k: v for k, v in inputs.items()
            if k not in [
                'uid', 'chain', 'submission_dir', 'argv',
                'original_cli', 'json_file',
            ]
        }
        try:
            _write_run_json(self, _get_initial_run_json_path(self.logs_path))
        except Exception as e:
            logger.warning(f'Failed to update run.json with input args: {e}')

    def get_result(self, result_key, default=None):
        return (self.results or {}).get(RUN_RESULTS_NAME, {}).get(result_key, default)

    def get_input(self, input_key: str, default=None):
        return self.inputs.get(input_key, default)

    def write_run_completed(
        self,
        results: Optional[dict[str, Any]] = None,
        revoked: Union[None, bool, str] = None,
        root_task_uuid: Optional[str] = None,
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
                revoked_reason = 'Run revoked (cancelled)'
            else:
                assert isinstance(revoked, str), f'Bad revoked: {type(revoked)}'
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

        completed_json_filepath = self._get_completion_run_json_path(self.logs_path)
        _write_run_json(self, completed_json_filepath)

        report_link = _run_json_link_path_from_logs_dir(self.logs_path)
        create_link(completed_json_filepath, report_link, relative=True)
        return report_link

    def as_serializable(self) -> dict[str, Any]:
        return convert_to_serializable(
            {
                f.name: getattr(self, f.name) for f in dataclasses.fields(self)
                if f.name != '_extra_fields'
            } | self._extra_fields
        )

    def chain_results(self) -> dict[str, Any]:
        assert self.results, 'Results not set; consider waiting for results.'
        return self.results[RUN_RESULTS_NAME]

    def get_failed_submitted_services(self) -> list[str]:
        assert self.results, 'Check for results before requesting failed services'
        # note these are just submitted, i.e. --chain,
        # services, not all failed services for the whole run.
        return (self.results.get(RUN_UNSUCCESSFUL_NAME) or {}).get('failed') or []

    def get_status_and_description(self) -> tuple['FireXRunStatus', str]:
        if not self.completed:
            return (FireXRunStatus.RUNNING, 'Run is still in progress.')

        if self.revoked:
            if self.revoked_details:
                revoked_description = self.revoked_details.get_description()
            else:
                revoked_description = 'Run cancelled without detailed reason.'
            return FireXRunStatus.REVOKED, revoked_description

        assert self.results, f'Expected completed, not-revoked run to have results, but none found. Check {self.logs_path}.'

        failed_services = self.get_failed_submitted_services()
        if failed_services:
            return FireXRunStatus.SOME_FAILED, ", ".join(failed_services)

        return FireXRunStatus.SUCCESS, 'Submitted services completed successfully.'

    def get_status(self) -> 'FireXRunStatus':
        return self.get_status_and_description()[0]

    def chain_has_service(
        self,
        query_services: Union[str, list[str]],
    ) -> bool:
        if self.chain is None:
            logger.debug(f'Run {self.firex_id} has no chain')
            return False
        services = [query_services] if isinstance(query_services, str) else query_services
        lower_chain = [ s.lower() for s in self.chain]
        return any(
            # just get the chain basename, not fully-qualified name
            query_s.split('.')[-1].lower() in lower_chain
            for query_s in services
        )

    @classmethod
    def _logs_dir_maybe_from_firex_id(
        cls,
        logs_dir: Optional[str]=None,
        firex_id: Optional[str]=None,
    ) -> str:
        if not logs_dir:
            assert firex_id, 'Must supply logs_dir or firex_id'
            return cls.run_logs_dir_from_firex_id(firex_id)
        return logs_dir

    @classmethod
    def is_run_json_complete(
        cls,
        logs_dir: Optional[str]=None,
        firex_id: Optional[str]=None,
        run_json_path: Optional[str]=None,
    ) -> bool:
        if run_json_path is not None:
            real_basename = os.path.basename(os.path.realpath(run_json_path))
            if real_basename == FireXJsonReportGenerator.completion_report_filename:
                return True

            if (
                not os.path.islink(run_json_path)
                and not logs_dir
                and not firex_id
            ):
                # need to read the file if it's not a symlink that will change to completion_report_filename
                try:
                    return cls.load_run_json_file(run_json_path).completed
                except OSError as e:
                    logger.warning(f'Failed to read {run_json_path} while checking completeness: {e}')
                    return False

        return os.path.exists(
            cls._get_completion_run_json_path(
                logs_dir, firex_id,
            )
        )

    @classmethod
    def set_revoked_if_incomplete(
        cls,
        logs_dir: Optional[str]=None,
        firex_id: Optional[str]=None,
        shutdown_reason: Optional[str]=None,
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
            logger.warning(f'Failed to maybe mark {logs_dir} complete: {e}')

    @classmethod
    def run_json_completed_time(
        cls,
        logs_dir: Optional[str]=None,
        firex_id: Optional[str]=None,
    ) -> Optional[datetime.datetime]:
        logs_dir = cls._logs_dir_maybe_from_firex_id(logs_dir, firex_id)
        if cls.is_run_json_complete(logs_dir=logs_dir):
            run_data = cls.load_from_logs_dir(logs_dir)
            if run_data.completed_timestamp:
                return run_data.completed_timestamp
            else:
                completed_json = cls._get_completion_run_json_path(
                        logs_dir=logs_dir)
                return pytz.utc.localize(
                    datetime.datetime.fromtimestamp(
                        os.path.getmtime(completed_json)),
                )
        return None

    @classmethod
    def wait_for_run_json_complete(
        cls,
        logs_dir: Optional[str]=None,
        firex_id: Optional[str]=None,
        timeout: int=0,
    ) -> bool:
        return wait_until(
            cls.is_run_json_complete,
            logs_dir=logs_dir,
            firex_id=firex_id,
            timeout=timeout,
            sleep_for=0.5,
        )

    def wait(self, timeout: int) -> bool:
        return self.wait_for_run_json_complete(
            logs_dir=self.logs_path,
            timeout=timeout,
        )

    def reload(self: T) -> T:
        return self.load_from_logs_dir(
            self.logs_path
        )

    def get_proc_duration(self) -> Optional[float]:
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
    root_task_uuid: Optional[str],
    completed_timestamp: Optional[datetime.datetime],
) -> Optional['RevokeDetails']:
    try:
        tracked_revoked_details = RevokeDetails.load_latest_run_revoke_details(
            logs_dir
        )
        # FIXME: should check the revoke request is recent enough to be the cause.
        if tracked_revoked_details:
            revoked_details = tracked_revoked_details
        elif shutdown_revoke_reason:
            revoked_details = RevokeDetails(
                logs_dir,
                reason=shutdown_revoke_reason,
                task_uuid=root_task_uuid or 'RUN-TASK',
                root_revoke=True,
                revoking_user=getuser(),
            )
        else:
            revoked_details = None

        if revoked_details:
            revoked_details.write_revoke_complete(completed_timestamp)
        return revoked_details
    except OSError:
        logger.exception('Failed to get completed revoke request')
    return None


class FireXRunStatus(str, enum.Enum):
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    SOME_FAILED = 'SOME_FAILED'
    REVOKED = 'REVOKED'

    def is_revoked(self) -> bool:
        return self == FireXRunStatus.REVOKED

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
    with NamedTemporaryFile(mode='w', encoding='utf-8', dir=os.path.dirname(report_file), delete=False) as f:
        json.dump(
            data.as_serializable(),
            fp=f,
            skipkeys=True,
            sort_keys=True,
            indent=4)
        f.flush()
        os.fsync(f.fileno())

    os.chmod(f.name, 0o644)
    os.replace(f.name, report_file)


def _run_json_link_path_from_logs_dir(logs_dir) -> str:
    return os.path.join(logs_dir, 'run.json')


class FireXJsonReportGenerator:

    reporter_dirname = 'json_reporter'
    initial_report_filename = 'initial_report.json'
    completion_report_filename = 'completion_report.json'

    @staticmethod
    def create_initial_run_json(
        uid,
        chain,
        submission_dir,
        argv,
        original_cli=None,
        json_file=None,
        **inputs,
    ) -> FireXRunData:
        run_info = FireXRunData.create_from_common_run_data(
            uid, chain, submission_dir, argv, original_cli, inputs,
            submit_proc_start_timestamp=datetime.datetime.fromtimestamp(
                psutil.Process().create_time(),
                tz=datetime.timezone.utc
            )
        )
        report_link = run_info.write_initial_run_json()
        if json_file:
            try:
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                logger.debug(f'{json_file} link already exist; '
                             f'post_run must have already created the link to {report_link}')

        return run_info

    @classmethod
    def create_completed_run_json(
        cls,
        uid: Optional[Uid]=None,
        run_revoked: bool=True,
        chain=None,
        root_id=None,
        submission_dir=None,
        argv=None,
        original_cli=None,
        json_file=None,
        logs_dir: Optional[str]=None,
        shutdown_reason: Optional[str]=None,
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
            logger.warning(f"Failed to read initial json for {logs_path}. Creating a minimal completion report.")
            if not uid:
                raise

            # best effort -- not all termination contexts have access to all this data :/
            run_info = FireXRunData.create_from_common_run_data(
                uid, chain, submission_dir, argv, original_cli, inputs,
            )

        report_link = run_info.write_run_completed(
            results=_get_run_results_from_root_task_promise(root_id, run_revoked, shutdown_reason),
            revoked=run_revoked and shutdown_reason,
            root_task_uuid=root_id.id if root_id else None,
        )

        if json_file:
            try:
                # This is typically not required, unless post_run ran before pre_run
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                pass # This is expected for most cases


def _get_run_results_from_root_task_promise(
    root_task_ar: Optional[AsyncResult],
    run_revoked: bool,
    shutdown_reason: Optional[str],
) -> dict[str, Any]:
    if root_task_ar and root_task_ar.successful():
        return get_results(root_task_ar)

    failures = []
    did_not_run = []
    if run_revoked or (root_task_ar and root_task_ar.state in [REVOKED, RETRY]):
        did_not_run = [
            f'was revoked (i.e. cancelled){f" due to: {shutdown_reason}" if shutdown_reason else ""}'
        ]
    elif root_task_ar and root_task_ar.failed():
        failures = [f"Run failed: {root_task_ar.result}"]
    else:
        failures = [f"Run failed before starting: {shutdown_reason or ''}"]

    return {
        RUN_RESULTS_NAME: {},
        RUN_UNSUCCESSFUL_NAME: create_unsuccessful_result(failures, did_not_run)
    }


def _get_initial_run_json_path(logs_dir):
    return os.path.join(
        logs_dir,
        FireXJsonReportGenerator.reporter_dirname,
        FireXJsonReportGenerator.initial_report_filename)


class ReporterStep(bootsteps.StartStopStep):

    def include_if(self, parent):
        return parent.hostname.startswith(app.conf.primary_worker_name + '@')

    def __init__(self, parent, **kwargs):
        self._logs_dir = None
        logfile = os.path.normpath(kwargs.get('logfile', '') or '')

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
                shutdown_reason='Celery stop bootstep unexpectedly found incomplete run',
            )


app.steps['worker'].add(ReporterStep)

# We want this step to finish after Pool at least (because a poolworker writes this file in the async case),
# but might as well finish after Hub too
Hub.requires = Hub.requires + (ReporterStep,)
