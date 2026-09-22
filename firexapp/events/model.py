import dataclasses
import datetime
import json
import logging
import os
import re
import secrets
import string
from collections import namedtuple
from enum import Enum
from pathlib import Path
from typing import Any

from firexapp.common import silent_mkdir
from firexapp.submit.uid import Uid
from firexkit.result import ChainInterruptedException

logger = logging.getLogger(__name__)


# event entry key name that allows a task to indicate a child-like relationship with another task.
# For example, a delayed-dependency that initiates a non-child task to run can indicate it is an ancestor
# of that triggered non-child task, since it effectively cause it to execute, much like an ordinary (i.e. celery)
# parent task.
ADDITIONAL_CHILDREN_KEY = "additional_children"
EXTERNAL_COMMANDS_KEY = "external_commands"

# event entry key name carrying why a task was revoked, sent with
# RunStates.REVOKE_COMPLETED so the reason can be shown against the task itself.
TASK_REVOKE_REASON_KEY = "revoke_reason"


class RunStates(Enum):
    RECEIVED = "task-received"
    STARTED = "task-started"
    BLOCKED = "task-blocked"
    UNBLOCKED = "task-unblocked"
    SUCCEEDED = "task-succeeded"
    FAILED = "task-failed"
    REVOKED = (
        "task-revoked"  # from celery, but "task-revoke-started" would be more accurate.
    )
    REVOKE_COMPLETED = "task-revoke-completed"
    INCOMPLETE = "task-incomplete"  # fake "forced to be completed" state.

    @classmethod
    def create(cls, v) -> "RunStates":
        if v == "task-started-info":
            v = "task-started"
        return RunStates(v)

    def get_priority(self) -> int:
        #
        # higher priority states are never overwritten by lower
        # priority states.
        # Newer equal priority states overwrite existing states.
        #
        return {
            self.INCOMPLETE: 1,
            self.REVOKED: 2,
            self.REVOKE_COMPLETED: 3,
            self.SUCCEEDED: 4,
        }.get(
            self,
            # many priorities are equal because any change is allowed,
            # including failure due to retries. Not great data modelling that failure is non-terminal.
            0,
        )

    def is_complete(
        self,
        # There are gotchas here, FAILED isn't "really" terminal
        # in the presence of retries, so allow callers to track
        # total task completion independently if they want complete accuracy.
        # by default failed is considered complete.
        has_completed: bool | None = None,
    ):
        complete_states = [
            RunStates.SUCCEEDED,
            RunStates.REVOKE_COMPLETED,
            RunStates.INCOMPLETE,
        ]
        if has_completed is None:
            complete_states.append(RunStates.FAILED)
        elif has_completed is True:
            complete_states += [RunStates.FAILED, RunStates.REVOKED]

        return self in complete_states

    def to_celery_event_type(self) -> str:
        return self.value

    def to_ui_state(self) -> str:
        # task states are now the same as their corresponding Celery event types.
        return self.to_celery_event_type()

    def is_revoke(self) -> bool:
        return self in [RunStates.REVOKE_COMPLETED, RunStates.REVOKED]

    @staticmethod
    def is_complete_state(task_state: Any, has_completed: bool | None = None) -> bool:
        try:
            return RunStates.create(task_state).is_complete(has_completed=has_completed)
        except ValueError:
            return False

    @staticmethod
    def is_incomplete_state(task_state: Any, has_completed: bool | None = None) -> bool:
        try:
            return not RunStates.create(task_state).is_complete(
                has_completed=has_completed
            )
        except ValueError:
            return False

    @staticmethod
    def get_forced_complete_celery_event_type(
        task_state: Any,
        has_completed: bool | None = None,
    ) -> str:
        try:
            state = RunStates.create(task_state)
        except ValueError:
            state = RunStates.INCOMPLETE
        else:
            if state == RunStates.REVOKED:
                state = RunStates.REVOKE_COMPLETED
            elif not state.is_complete(has_completed=has_completed):
                state = RunStates.INCOMPLETE

        return state.to_celery_event_type()

    @staticmethod
    def get_higher_priority_state(
        existing_state_str: str | None,
        new_state_str: str | None,
    ) -> str:
        try:
            existing_state = RunStates.create(existing_state_str)
        except ValueError:
            existing_state = None

        try:
            new_state = RunStates.create(new_state_str)
        except ValueError:
            new_state = None

        chosen_state: RunStates
        if existing_state and new_state:
            existing_prio = existing_state.get_priority()
            new_prio = new_state.get_priority()
            if new_prio >= existing_prio:
                chosen_state = new_state
            else:
                chosen_state = existing_state
        elif existing_state:
            chosen_state = existing_state
        elif new_state:
            chosen_state = new_state
        else:
            return new_state_str or ""

        return chosen_state.to_celery_event_type()


COMPLETE_RUNSTATES = {s.to_celery_event_type() for s in RunStates if s.is_complete()}


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _revoke_serializer(obj):
    if isinstance(obj, datetime.datetime):
        # Convert datetime to ISO 8601 string
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


@dataclasses.dataclass
class RevokeDetails:
    """
    Why, and by whom, a task (or an entire run) was revoked.

    Each revoke request is written to its own file under
    <logs_dir>/debug/revoke_requests/, so that processes other than the revoker --
    the worker running the revoked task, the run.json writer, report generators --
    can explain the revoke afterwards.

    The logs_dir-keyed entry points here exist for the contexts that have no app
    (e.g. writing run.json during shutdown); everything else should go through the
    FireXCelery revoke methods.
    """

    #
    # ensure loading is backwards compatible
    #
    logs_dir: str
    reason: str | None  # FIXME: try to make this always set.
    task_uuid: str
    root_revoke: bool
    revoking_user: str | None = None
    revoke_start_time: datetime.datetime = dataclasses.field(default_factory=_now_utc)
    revoke_complete_time: datetime.datetime | None = None
    _id: str | None = None

    def is_revoke_completed(self) -> bool:
        return self.revoke_complete_time is not None

    def write_revoke_complete(
        self, revoke_complete_time: datetime.datetime | None = None
    ):
        if self.revoke_complete_time is None:
            self.revoke_complete_time = revoke_complete_time or _now_utc()
            self.write()

    def get_description(self) -> str:
        user_msg = f" by {self.revoking_user}" if self.revoking_user else ""
        description = (
            f"Run was revoked (cancelled){user_msg} with reason: {self.reason}"
        )
        if not description.endswith("."):
            description += "."
        return description

    def write(self):
        if not self._id:
            self._id = "".join(secrets.choice(string.ascii_lowercase) for _ in range(6))

        scope_detail = "run-revoke" if self.root_revoke else "task-revoke"
        file = os.path.join(
            RevokeDetails._get_run_revoke_dir(self.logs_dir),
            f"{scope_detail}:{self.task_uuid}:{self._id}.json",
        )
        try:
            with open(file, "w", encoding="utf-8") as fp:
                json.dump(
                    dataclasses.asdict(self), fp, default=_revoke_serializer, indent=4
                )
        except OSError:
            logger.exception("Failed to write revoke request.")

    @staticmethod
    def complete_task_revoke(
        logs_dir: str,
        task_uuid: str,
    ) -> "RevokeDetails | None":
        """
        Marks every revoke request naming task_uuid complete, and returns the
        details explaining why that task was revoked.

        A task revoked only because the whole run was revoked has no request of its
        own, so the run's revoke request is what explains it; both come out of the
        single directory listing this needs anyway.
        """
        all_revoke_req_files = RevokeDetails._revoke_request_files(logs_dir)

        # Both scopes: revoking the root task is how an entire run is revoked, so the
        # root task's own revoke request is a run-revoke one.
        task_revoke_files = RevokeDetails._select_revoke_requests(
            all_revoke_req_files,
            task_uuid=task_uuid,
        )
        # Newest first, established before write_revoke_complete() rewrites (and so
        # re-times) the files.
        task_revoke_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

        latest_details = None
        for f in task_revoke_files:
            details = RevokeDetails._load(f)
            details.write_revoke_complete()
            latest_details = latest_details or details

        if latest_details:
            return latest_details

        return RevokeDetails._load_latest(
            RevokeDetails._select_revoke_requests(
                all_revoke_req_files,
                run_revoked=True,
            )
        )

    @staticmethod
    def load_latest_run_revoke_details(logs_dir: str) -> "RevokeDetails | None":
        return RevokeDetails.load_latest_revoke_details(
            logs_dir,
            run_revoked=True,
        )

    @staticmethod
    def load_latest_revoke_details(
        logs_dir: str,
        run_revoked=False,
        task_uuid=None,
    ) -> "RevokeDetails | None":
        return RevokeDetails._load_latest(
            RevokeDetails._select_revoke_requests(
                RevokeDetails._revoke_request_files(logs_dir),
                run_revoked=run_revoked,
                task_uuid=task_uuid,
            )
        )

    @staticmethod
    def _get_run_revoke_dir(logs_dir: str) -> str:
        run_revoke_dir = os.path.join(logs_dir, Uid.debug_dirname, "revoke_requests")
        silent_mkdir(run_revoke_dir)
        return run_revoke_dir

    @staticmethod
    def _revoke_request_files(logs_dir: str) -> list[Path]:
        revoke_reqs_dir = Path(RevokeDetails._get_run_revoke_dir(logs_dir))
        return [
            f
            for f in revoke_reqs_dir.iterdir()
            if f.is_file() and f.name.endswith(".json")
        ]

    @staticmethod
    def _select_revoke_requests(
        revoke_req_files: list[Path],
        run_revoked=None,
        task_uuid=None,
    ) -> list[Path]:
        if run_revoked is None:
            query_prefixes = ("run-revoke:", "task-revoke:")
        elif run_revoked:
            query_prefixes = ("run-revoke:",)
        else:
            query_prefixes = ("task-revoke:",)

        return [
            f
            for f in revoke_req_files
            if (
                f.name.startswith(query_prefixes)
                and (task_uuid is None or f":{task_uuid}:" in f.name)
            )
        ]

    @staticmethod
    def _load_latest(revoke_req_files: list[Path]) -> "RevokeDetails | None":
        if not revoke_req_files:
            return None
        return RevokeDetails._load(
            max(revoke_req_files, key=lambda f: f.stat().st_mtime),
        )

    @staticmethod
    def _load(revoke_req_file: Path) -> "RevokeDetails":
        data_dict = json.loads(
            revoke_req_file.read_text(encoding="utf-8"),
        )
        if data_dict["revoke_start_time"]:
            data_dict["revoke_start_time"] = datetime.datetime.fromisoformat(
                data_dict["revoke_start_time"]
            )
        if data_dict["revoke_complete_time"]:
            data_dict["revoke_complete_time"] = datetime.datetime.fromisoformat(
                data_dict["revoke_complete_time"]
            )
        return RevokeDetails(**data_dict)


class RunMetadataColumn(Enum):
    FIREX_ID = "firex_id"
    LOGS_DIR = "logs_dir"
    CHAIN = "chain"
    ROOT_UUID = "root_uuid"
    FIREX_REQUESTER = "firex_requester"


FireXRunMetadata = namedtuple(
    "RunMetadata",
    # must be in sync with RunMetadataColumn, including order.
    ["firex_id", "logs_dir", "chain", "root_uuid", "firex_requester"],
)


# Note field order matters. TaskColumn is the authority on field order.
class TaskColumn(Enum):
    UUID = "uuid"
    FIREX_ID = "firex_id"
    CHAIN_DEPTH = "chain_depth"
    BOUND_ARGS = "firex_bound_args"
    RESULTS = "firex_result"
    DEFAULT_BOUND_ARGS = "firex_default_bound_args"
    FROM_PLUGIN = "from_plugin"
    HOSTNAME = "hostname"
    LOGS_URL = "logs_url"
    LONG_NAME = "long_name"
    NAME = "name"
    ACTUAL_RUNTIME = "actual_runtime"
    FIRST_STARTED = "first_started"
    PARENT_ID = "parent_id"
    RETRIES = "retries"
    STATE = "state"
    TASK_NUM = "task_num"
    UTCOFFSET = "utcoffset"
    EXCEPTION = "exception"
    TRACEBACK = "traceback"
    EXCEPTION_CAUSE_UUID = "exception_cause_uuid"


TASK_COLUMN_NAMES = [tc.value for tc in TaskColumn]


def get_task_data(input_dict):
    return {k: v for k, v in input_dict.items() if k in TASK_COLUMN_NAMES}


FireXTask = namedtuple(
    "FireXTask",
    [
        # MUST BE SAME ORDER AS TaskColumn
        "uuid",
        "firex_id",
        "chain_depth",
        "firex_bound_args",
        "firex_result",
        "firex_default_bound_args",
        "from_plugin",
        "hostname",
        "logs_url",
        "long_name",
        "name",
        "actual_runtime",
        "first_started",
        "parent_id",
        "retries",
        "state",
        "task_num",
        "utcoffset",
        "exception",
        "traceback",
        "exception_cause_uuid",
    ],
)


def is_chain_exception(task):
    return task.exception and task.exception.strip().startswith(
        ChainInterruptedException.__name__
    )


def get_chain_exception_child_uuid(task):
    assert is_chain_exception(task)
    exception_str = task.exception.strip()
    # example: ChainInterruptedException('ad9b0b79-86e9-4d76-8654-9c19886d50a1', ...).
    m = re.search(
        r"" + ChainInterruptedException.__name__ + r"\('([\da-f\-]+)'", exception_str
    )
    assert m, f"No UUID found in {exception_str}."
    return m.group(1)


def is_failed(task: FireXTask, ignore_chain_exception=False):
    is_failure = task.state == RunStates.FAILED.value
    if not is_failure:
        return False

    if ignore_chain_exception:
        return not is_chain_exception(task)

    return True
