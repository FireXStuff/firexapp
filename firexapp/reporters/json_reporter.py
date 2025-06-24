import json
import os
from socket import gethostname
import dataclasses
from tempfile import NamedTemporaryFile
from typing import Union, Optional, Any, TypeVar, Type
import datetime
import enum
import pytz
from pathlib import Path
from getpass import getuser
import string
import secrets


from celery import bootsteps
from celery.worker.components import Hub

from firexapp.application import get_app_tasks
from firexapp.common import silent_mkdir, create_link, wait_until
from firexapp.submit.uid import FIREX_ID_REGEX, Uid
from firexkit.result import (
    get_run_results_from_root_task_promise,
    RUN_RESULTS_NAME,
    RUN_UNSUCCESSFUL_NAME,
)
from firexkit.task import convert_to_serializable
from celery.utils.log import get_task_logger
from firexapp.engine.celery import app

logger = get_task_logger(__name__)

T = TypeVar('T', bound='FireXRunData')

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

    _extra_fields: dict[str, Any] = dataclasses.field(default_factory=dict)
    _loaded_logs_dir: Optional[str] = None

    @staticmethod
    def create_from_common_run_data(
        uid: Uid,
        chain,
        submission_dir,
        argv,
        original_cli,
        inputs: dict[str, Any],
    ) -> 'FireXRunData':
        if chain:
            chain = [t.short_name for t in get_app_tasks(chain)]

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
        )

    @classmethod
    def run_logs_dir_from_firex_id(cls: Type[T], firex_id: str) -> str:
        raise NotImplementedError()

    @classmethod
    def _create_from_dict(
        cls: Type[T],
        run_dict: dict[str, Any],
        # this is bad, but some UT has fake logs_path entries,
        # so we need to track the logs_dir that the run.json was
        # loaded from. Too many testcases to fix right now.
        _loaded_logs_dir=None,
    ) -> T:
        field_names = {
            f.name for f in dataclasses.fields(cls)
            if f.name not in ['_extra_fields', '_loaded_logs_dir']
        }
        modelled_fields = {
            k: v for k, v in run_dict.items()
            if k in field_names
        }
        exclude_from_extra = field_names | {'_loaded_logs_dir'}
        extra_fields = {
            k: v for k, v in run_dict.items()
            if k not in exclude_from_extra
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
            _loaded_logs_dir=_loaded_logs_dir)

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
        _loaded_logs_dir=None,
    ) -> T:
        with open(json_filepath, encoding='utf-8') as f:
            return cls._create_from_dict(
                json.load(fp=f),
                _loaded_logs_dir=_loaded_logs_dir,
            )

    @classmethod
    def load_initial(cls: Type[T], logs_dir: str) -> T:
        return cls.load_run_json_file(
            _get_initial_run_json_path(logs_dir),
            _loaded_logs_dir=logs_dir,
        )

    @classmethod
    def load_from_logs_dir(cls: Type[T], logs_dir: str) -> T:
        try:
            return cls.load_run_json_file(
                cls._get_completion_run_json_path(logs_dir),
                _loaded_logs_dir=logs_dir,
            )
        except FileNotFoundError:
            try:
                return cls.load_initial(logs_dir)
            except FileNotFoundError:
                return cls.load_run_json_file(
                    # This should be impossible but we have test data
                    # for this impossible case :/
                    _run_json_link_path_from_logs_dir(logs_dir),
                    _loaded_logs_dir=logs_dir,
                )

    @classmethod
    def load_from_firex_id(cls: Type[T], firex_id: str) -> T:
        logs_dir = cls.run_logs_dir_from_firex_id(firex_id)
        return cls.load_from_logs_dir(logs_dir)

    def _logs_dir(self) -> str:
        return self._loaded_logs_dir or self.logs_path

    def write_initial_run_json(self) -> str:
        init_json_filepath = _get_initial_run_json_path(self._logs_dir())
        _write_run_json(self, init_json_filepath)
        return init_json_filepath

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
            now = _now_utc()
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
                    self._logs_dir(),
                    revoked_reason,
                    root_task_uuid,
                    now,
                )

        completed_json_filepath = self._get_completion_run_json_path(self._logs_dir())
        _write_run_json(self, completed_json_filepath)
        return completed_json_filepath

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

        assert self.results, f'Expected completed, not-revoked run to have results, but none found. Check {self._logs_dir()}.'

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
    ) -> bool:
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
            FireXRunData.is_run_json_complete,
            logs_dir=logs_dir,
            firex_id=firex_id,
            timeout=timeout,
            sleep_for=0.5,
        )

    def wait(self, timeout: int) -> bool:
        return self.wait_for_run_json_complete(
            logs_dir=self._logs_dir(),
            timeout=timeout,
        )

    def reload(self: T) -> T:
        return self.load_from_logs_dir(
            self._logs_dir()
        )

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


def _revoke_serializer(obj):
    if isinstance(obj, datetime.datetime):
        # Convert datetime to ISO 8601 string
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

@dataclasses.dataclass
class RevokeDetails:
    #
    # ensure loading is backwards compatible
    #
    logs_dir: str
    reason: str
    task_uuid: str
    root_revoke: bool
    revoking_user: Optional[str] = None
    revoke_start_time: datetime.datetime = dataclasses.field(default_factory=_now_utc)
    revoke_complete_time: Optional[datetime.datetime] = None
    _id: Optional[str] = None

    def is_revoke_completed(self) -> bool:
        return self.revoke_complete_time is not None

    def write_revoke_complete(self, revoke_complete_time: Optional[datetime.datetime]=None):
        if self.revoke_complete_time is None:
            self.revoke_complete_time = revoke_complete_time or _now_utc()
            self.write()

    def get_description(self) -> str:
        user_msg = f' by {self.revoking_user}' if self.revoking_user else ''
        description = f'Run was revoked (cancelled){user_msg} with reason: {self.reason}'
        if not description.endswith('.'):
            description += '.'
        return description

    def write(self):
        if not self._id:
            self._id = ''.join(secrets.choice(string.ascii_lowercase) for _ in range(6))

        scope_detail = 'run-revoke' if self.root_revoke else 'task-revoke'
        file = os.path.join(
            RevokeDetails._get_run_revoke_dir(self.logs_dir),
            f'{scope_detail}:{self.task_uuid}:{self._id}.json',
        )
        try:
            with open(file, 'tw', encoding='utf-8') as fp:
                json.dump(
                    dataclasses.asdict(self),
                    fp,
                    default=_revoke_serializer,
                    indent=4)
        except OSError:
            logger.exception('Failed to write revoke request.')

    @staticmethod
    def write_task_revoke_complete(logs_dir: str, task_uuid: str):
        task_revoke_files = RevokeDetails._find_revoke_request(
            logs_dir, task_uuid=task_uuid)
        for f in task_revoke_files:
            RevokeDetails._load(f).write_revoke_complete()

    @staticmethod
    def _get_run_revoke_dir(logs_dir: str) -> str:
        run_revoke_dir = os.path.join(logs_dir, Uid.debug_dirname, 'revoke_requests')
        silent_mkdir(run_revoke_dir)
        return run_revoke_dir

    @staticmethod
    def _find_revoke_request(logs_dir: str, run_revoked=None, task_uuid=None) -> list[Path]:
        revoke_reqs_dir = Path(RevokeDetails._get_run_revoke_dir(logs_dir))
        if run_revoked is None:
            query_prefixes = ['run-revoke:', 'task-revoke:']
        elif run_revoked:
            query_prefixes = ['run-revoke:']
        else:
            query_prefixes = ['task-revoke:']

        return [
            f for f in revoke_reqs_dir.iterdir()
            if (
                f.is_file()
                and f.name.endswith('.json')
                and any(
                    f.name.startswith(prefix)
                    for prefix in query_prefixes
                )
                and (
                    task_uuid is None
                    or f':{task_uuid}:' in f.name
                )
            )
        ]

    @staticmethod
    def _load(revoke_req_file: Path) -> 'RevokeDetails':
        data_dict = json.loads(
            revoke_req_file.read_text(encoding='utf-8'),
        )
        if data_dict['revoke_start_time']:
            data_dict['revoke_start_time'] = datetime.datetime.fromisoformat(
                data_dict['revoke_start_time']
            )
        if data_dict['revoke_complete_time']:
            data_dict['revoke_complete_time'] = datetime.datetime.fromisoformat(
                data_dict['revoke_complete_time']
            )
        return RevokeDetails(**data_dict)

    @staticmethod
    def load_latest_run_revoke_details(logs_dir: str) -> Optional['RevokeDetails']:
        run_revoke_req_files = RevokeDetails._find_revoke_request(
            logs_dir, run_revoked=True,
        )
        if run_revoke_req_files:
            latest_root_revoke_file = max(
                run_revoke_req_files,
                key=lambda f: f.stat().st_mtime,
            )
            return RevokeDetails._load(latest_root_revoke_file)

        return None


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
    formatters = ('json',)

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
    ):
        run_info = FireXRunData.create_from_common_run_data(
            uid, chain, submission_dir, argv, original_cli, inputs,
        )
        initial_report_file = run_info.write_initial_run_json()

        report_link = _run_json_link_path_from_logs_dir(uid.logs_dir)
        try:
            create_link(initial_report_file, report_link, delete_link=False)
        except FileExistsError:
            logger.debug(f'f{report_link} link already exist. '
                         f'No need to link to f{initial_report_file}')

        if json_file:
            try:
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                logger.debug(f'{json_file} link already exist; '
                             f'post_run must have already created the link to {report_link}')

    @classmethod
    def create_completed_run_json(
        cls,
        uid: Optional[Uid]=None,
        run_revoked=True,
        chain=None,
        root_id=None,
        submission_dir=None,
        argv=None,
        original_cli=None,
        json_file=None,
        logs_dir: Optional[str]=None,
        shutdown_reason=None,
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

        completion_report_file = run_info.write_run_completed(
            results=get_run_results_from_root_task_promise(root_id),
            revoked=run_revoked and shutdown_reason,
            root_task_uuid=root_id.id if root_id else None,
        )

        report_link = _run_json_link_path_from_logs_dir(logs_path)
        create_link(completion_report_file, report_link, relative=True)

        if json_file:
            try:
                # This is typically not required, unless post_run ran before pre_run
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                # This is expected for most cases
                pass


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
