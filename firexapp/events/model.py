from collections import namedtuple
from enum import Enum
import logging


logger = logging.getLogger(__name__)


class RunStates(Enum):
    RECEIVED = "task-received"
    STARTED = "task-started"
    BLOCKED = "task-blocked"
    UNBLOCKED = "task-unblocked"
    SUCCEEDED = "task-succeeded"
    FAILED = "task-failed"
    REVOKED = "task-revoked"
    INCOMPLETE = "task-incomplete"


ALL_RUNSTATES = {
    RunStates.RECEIVED.value: {'terminal': False},
    RunStates.STARTED.value: {'terminal': False},
    RunStates.BLOCKED.value: {'terminal': False},
    RunStates.UNBLOCKED.value: {'terminal': False},
    RunStates.SUCCEEDED.value: {'terminal': True},
    RunStates.FAILED.value: {'terminal': True},
    RunStates.REVOKED.value: {'terminal': True},
    RunStates.INCOMPLETE.value: {'terminal': True},  # server-side kludge state to fix tasks that will never complete.
}
COMPLETE_RUNSTATES = [s for s, v in ALL_RUNSTATES.items() if v['terminal']]
INCOMPLETE_RUNSTATES = [s for s, v in ALL_RUNSTATES.items() if not v['terminal']]


class RunMetadataColumn(Enum):
    FIREX_ID = "firex_id"
    LOGS_DIR = "logs_dir"
    CHAIN = "chain"
    ROOT_UUID = "root_uuid"


RunMetadata = namedtuple('RunMetadata', [RunMetadataColumn.FIREX_ID.value,
                                         RunMetadataColumn.LOGS_DIR.value,
                                         RunMetadataColumn.CHAIN.value])


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


TASK_COLUMN_NAMES = [tc.value for tc in TaskColumn]


def get_task_data(input_dict):
    return {k: v for k, v in input_dict.items() if k in TASK_COLUMN_NAMES}


FireXTask = namedtuple('FireXTask', [tc.value for tc in TaskColumn])
