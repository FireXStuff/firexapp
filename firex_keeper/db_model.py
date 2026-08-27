from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Boolean, Float, Text, Index
from sqlalchemy.types import JSON

from firexapp.events.model import RunMetadataColumn, TaskColumn


UUID_LEN = 37
HOSTNAME_LEN = 40
LOGS_URL_LEN = 200
EXCEPTION_LEN = 200


metadata = MetaData()
firex_run_metadata = Table(
    'firex_run_metadata',
    metadata,
    Column(RunMetadataColumn.FIREX_ID.value, String(50), primary_key=True, nullable=False),
    Column(RunMetadataColumn.LOGS_DIR.value, String(300), nullable=False),
    Column(RunMetadataColumn.CHAIN.value, String(70), nullable=False),
    Column(RunMetadataColumn.ROOT_UUID.value, String(UUID_LEN), nullable=True),
    Column('keeper_complete', Boolean, default=False),
)

COLS_TO_SQLALCHEMY_CONFIG = {
    # TODO: consider making uuid an integer, converted before & after write.
    TaskColumn.UUID: {'kwargs': {'type_': String(UUID_LEN), 'primary_key': True}},
    TaskColumn.FIREX_ID: {'kwargs': {'type_': None}, 'args': [ForeignKey('firex_run_metadata.firex_id')]},
    TaskColumn.CHAIN_DEPTH: {'kwargs': {'type_': Integer}},
    TaskColumn.BOUND_ARGS: {'kwargs': {'type_': JSON, 'default': None}},
    TaskColumn.RESULTS: {'kwargs': {'type_': JSON, 'default': None}},
    TaskColumn.DEFAULT_BOUND_ARGS: {'kwargs': {'type_': JSON, 'default': None}},
    TaskColumn.FROM_PLUGIN: {'kwargs': {'type_': Boolean, 'default': None}},
    TaskColumn.HOSTNAME: {'kwargs': {'type_': String(HOSTNAME_LEN), 'default': None}},
    TaskColumn.LOGS_URL: {'kwargs': {'type_': String(LOGS_URL_LEN), 'default': None}},
    TaskColumn.LONG_NAME: {'kwargs': {'type_': String(100)}},
    TaskColumn.NAME: {'kwargs': {'type_': String(30)}},
    TaskColumn.ACTUAL_RUNTIME: {'kwargs': {'type_': Float, 'default': None}},
    TaskColumn.FIRST_STARTED: {'kwargs': {'type_': Float, 'default': None}},
    TaskColumn.PARENT_ID: {'kwargs': {'type_': String(UUID_LEN)}},
    TaskColumn.RETRIES: {'kwargs': {'type_': Integer, 'default': None}},
    TaskColumn.STATE: {'kwargs': {'type_': String(15), 'default': None}},
    TaskColumn.TASK_NUM: {'kwargs': {'type_': Integer, 'default': None}},
    TaskColumn.UTCOFFSET: {'kwargs': {'type_': Integer, 'default': None}},
    TaskColumn.EXCEPTION: {'kwargs': {'type_': String(EXCEPTION_LEN), 'default': None}},
    TaskColumn.TRACEBACK: {'kwargs': {'type_': Text, 'default': None}},
    TaskColumn.EXCEPTION_CAUSE_UUID: {'kwargs': {'type_': String(UUID_LEN), 'default': None}},
}

# Note SQL columns must be in same order as FireXTask namedtuple fields in order to create FireXTasks from query
# results. The TaskColumn enum is the authority on column/field order.
TASK_COLUMNS = [Column(tc.value,
                       *COLS_TO_SQLALCHEMY_CONFIG[tc].get('args', []),
                       **COLS_TO_SQLALCHEMY_CONFIG[tc]['kwargs'])
                for tc in TaskColumn
                if tc in COLS_TO_SQLALCHEMY_CONFIG]

TASKS_TABLENAME = 'firex_tasks'
firex_tasks = Table(TASKS_TABLENAME, metadata, *TASK_COLUMNS)

Index("task_parent_index", firex_tasks.c.parent_id) # for descendants query
Index("task_name_index", firex_tasks.c.name) # commonly queried