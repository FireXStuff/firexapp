from collections import namedtuple
from enum import Enum
import logging
import re

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


TASK_COLUMN_NAMES = [tc.value for tc in TaskColumn]


def get_task_data(input_dict):
    return {k: v for k, v in input_dict.items() if k in TASK_COLUMN_NAMES}


FireXTask = namedtuple('FireXTask', [tc.value for tc in TaskColumn])


def is_chain_exception(task):
    return task.exception and task.exception.strip().startswith(ChainInterruptedException.__name__)


def get_chain_exception_child_uuid(task):
    assert is_chain_exception(task)
    exception_str = task.exception.strip()
    # example: ChainInterruptedException('package.module.Service[ad9b0b79-86e9-4d76-8654-9c19886d50a1]',).
    m = re.search(r'' + ChainInterruptedException.__name__ + "\('.*\[(.*?)\]'", exception_str)
    assert m, "No UUID found in %s." % exception_str
    return m.group(1)


def is_failed(task: FireXTask, ignore_chain_exception=False):
    is_failure = task.state == RunStates.FAILED.value
    if not is_failure:
        return False

    if ignore_chain_exception:
        return not is_chain_exception(task)

    return True
