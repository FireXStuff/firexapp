import json
import os
import dataclasses
from typing import Optional
import datetime
from pathlib import Path
import string
import secrets

from firexapp.common import silent_mkdir
from firexapp.submit.uid import Uid
from celery.utils.log import get_task_logger


logger = get_task_logger(__name__)


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

def _revoke_serializer(obj):
    if isinstance(obj, datetime.datetime):
        # Convert datetime to ISO 8601 string
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")