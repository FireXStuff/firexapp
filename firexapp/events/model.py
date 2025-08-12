from collections import namedtuple
from enum import Enum
import logging
import re
from typing import Optional, Any

from firexkit.result import ChainInterruptedException

logger = logging.getLogger(__name__)


# event entry key name that allows a task to indicate a child-like relationship with another task.
# For example, a delayed-dependency that initiates a non-child task to run can indicate it is an ancestor
# of that triggered non-child task, since it effectively cause it to execute, much like an ordinary (i.e. celery)
# parent task.
ADDITIONAL_CHILDREN_KEY = 'additional_children'
EXTERNAL_COMMANDS_KEY = 'external_commands'


class RunStates(Enum):
    RECEIVED = "task-received"
    STARTED = "task-started"
    BLOCKED = "task-blocked"
    UNBLOCKED = "task-unblocked"
    SUCCEEDED = "task-succeeded"
    FAILED = "task-failed"
    REVOKED = "task-revoked" # from celery, but "task-revoke-started" would be more accurate.
    REVOKE_COMPLETED = 'task-revoke-completed'
    INCOMPLETE = "task-incomplete" # fake "forced to be completed" state.

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
            0)

    def is_complete(
        self,
        # There are gotchas here, FAILED isn't "really" terminal
        # in the presence of retries, so allow callers to track
        # total task completion independently if they want complete accuracy.
        # by default failed is considered complete.
        has_completed: Optional[bool]=None,
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
    def is_complete_state(task_state: Any, has_completed: Optional[bool]=None) -> bool:
        try:
            return RunStates(task_state).is_complete(has_completed=has_completed)
        except ValueError:
            return False

    @staticmethod
    def is_incomplete_state(task_state: Any, has_completed: Optional[bool]=None) -> bool:
        try:
            return not RunStates(task_state).is_complete(has_completed=has_completed)
        except ValueError:
            return False

    @staticmethod
    def get_forced_complete_celery_event_type(
        task_state: Any,
        has_completed: Optional[bool]=None,
    ) -> str:
        try:
            state = RunStates(task_state)
        except ValueError:
            state = RunStates.INCOMPLETE
        else:
            if state == RunStates.REVOKED:
                state =  RunStates.REVOKE_COMPLETED
            elif not state.is_complete(has_completed=has_completed):
                state = RunStates.INCOMPLETE

        return state.to_celery_event_type()

    @staticmethod
    def get_higher_priority_state(
        existing_state_str: Optional[str],
        new_state_str: Optional[str],
    ) -> str:
        try:
            existing_state = RunStates(existing_state_str)
        except ValueError:
            existing_state = None

        try:
            new_state = RunStates(new_state_str)
        except ValueError:
            new_state = None

        chosen_state : RunStates
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
            return new_state_str or ''

        return chosen_state.to_celery_event_type()


COMPLETE_RUNSTATES = {
    s.to_celery_event_type() for s in RunStates if s.is_complete()
}

class RunMetadataColumn(Enum):
    FIREX_ID = "firex_id"
    LOGS_DIR = "logs_dir"
    CHAIN = "chain"
    ROOT_UUID = "root_uuid"
    FIREX_REQUESTER = "firex_requester"


FireXRunMetadata = namedtuple('RunMetadata', [rmc.value for rmc in RunMetadataColumn])


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
    EXCEPTION_CAUSE_UUID = 'exception_cause_uuid'


TASK_COLUMN_NAMES = [tc.value for tc in TaskColumn]


def get_task_data(input_dict):
    return {k: v for k, v in input_dict.items() if k in TASK_COLUMN_NAMES}


FireXTask = namedtuple('FireXTask', [
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
    'exception_cause_uuid',
])


def is_chain_exception(task):
    return task.exception and task.exception.strip().startswith(ChainInterruptedException.__name__)


def get_chain_exception_child_uuid(task):
    assert is_chain_exception(task)
    exception_str = task.exception.strip()
    # example: ChainInterruptedException('ad9b0b79-86e9-4d76-8654-9c19886d50a1', ...).
    m = re.search(r'' + ChainInterruptedException.__name__ + "\('([\da-f\-]+)'", exception_str)
    assert m, "No UUID found in %s." % exception_str
    return m.group(1)


def is_failed(task: FireXTask, ignore_chain_exception=False):
    is_failure = task.state == RunStates.FAILED.value
    if not is_failure:
        return False

    if ignore_chain_exception:
        return not is_chain_exception(task)

    return True
