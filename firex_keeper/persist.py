import json
import logging
import os
from collections.abc import Sequence
from contextlib import contextmanager
from sqlite3 import OperationalError as SqlLiteOperationalError
from time import perf_counter, sleep

from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import and_, select
from sqlalchemy.sql.selectable import Select

from firex_keeper.db_model import firex_run_metadata, firex_tasks, metadata
from firexapp.common import wait_until
from firexapp.events.model import COMPLETE_RUNSTATES, FireXRunMetadata, FireXTask
from firexapp.submit.uid import Uid

logger = logging.getLogger(__name__)


class FireXWaitQueryExceeded(Exception):
    pass


def task_by_uuid_exp(task_uuid):
    return firex_tasks.c.uuid == task_uuid


def task_uuid_complete_exp(task_uuid):
    return and_(
        firex_tasks.c.uuid == task_uuid, firex_tasks.c.state.in_(COMPLETE_RUNSTATES)
    )


def cur_task_by_uuid_exp():
    from celery import current_task

    if not current_task:
        return False
    return task_by_uuid_exp(current_task.request.id)


def _custom_json_loads(*args, **kwargs):
    # JSON columns can still store ints in sqlite, so pass these values along even though they can't be decoded as
    # JSON.
    if isinstance(args[0], int):
        return args[0]
    return json.loads(*args, **kwargs)


# Keeper's writer thread holds the only read-write connection to the keeper DB, which is
# often on NFS. Every other connection must be read-only: a read-write connection rolls
# back any journal it believes is hot, and if it can't see the writer's NFS lock, it rolls
# back a live transaction and corrupts the DB.
# WAL must not be used while other hosts read the live DB: its wal-index is shared memory,
# which isn't coherent across hosts. synchronous must stay FULL (the default): NORMAL can
# corrupt rollback-journal DBs on power loss.
KEEPER_PAGE_SIZE = 16384
KEEPER_WRITER_PRAGMAS = (
    # Only applies when the DB is created. Task rows average ~20KB of JSON, so 4K pages put
    # most rows in overflow page chains, each page a serial NFS round trip for readers.
    f"page_size={KEEPER_PAGE_SIZE}",
    # Commit by truncating the journal instead of creating and deleting it, saving NFS round
    # trips per commit. Readers treat an empty journal as absent. Not PERSIST: readers pay
    # extra round trips when the journal isn't empty.
    "journal_mode=TRUNCATE",
    # 64MiB keeps aggregation lookups of written tasks off NFS. With a single writer, the
    # cache stays valid across transactions.
    "cache_size=-65536",
    # A commit waits for in-progress reads to finish, and slow cross-site reads easily exceed
    # the 5s default, rolling back the whole batch. The waiting writer blocks new readers, so
    # it can't be starved.
    "busy_timeout=60000",
)
READER_PRAGMAS = ("query_only=1",)


def _execute_pragmas_on_connect(engine, pragmas):
    # Pragmas are per DBAPI connection, and the engine opens a new one per connect().
    @event.listens_for(engine, "connect")
    def _execute_pragmas(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        for pragma in pragmas:
            cursor.execute(f"PRAGMA {pragma}")
        cursor.close()


def _db_connection_str(db_file, read_only, is_run_complete=False):
    db_conn_str = f"sqlite:///file:{db_file}"

    params = {"uri": "true"}

    if is_run_complete:
        params["immutable"] = "1"
        read_only = True

    if read_only:
        params["mode"] = "ro"

    db_conn_str += "?" + "&".join([f"{k}={v}" for k, v in params.items()])

    return db_conn_str


def connect_db(
    db_file,
    read_only=False,
    metadata_to_create=metadata,
    is_run_complete=False,
    pragmas: Sequence[str] = (),
):
    engine = create_engine(
        _db_connection_str(db_file, read_only, is_run_complete=is_run_complete),
        json_deserializer=_custom_json_loads,
    )
    pragmas = (*pragmas, *(READER_PRAGMAS if read_only or is_run_complete else ()))
    if pragmas:
        _execute_pragmas_on_connect(engine, pragmas)

    if not os.path.exists(db_file):
        logger.info(f"Creating schema for {db_file}")
        metadata_to_create.create_all(engine)
        logger.info(f"Schema creation complete for {db_file}")

    return engine.connect()


def get_db_file_dir_path(logs_dir: str) -> str:
    return os.path.join(logs_dir, Uid.debug_dirname, "keeper")


def get_keeper_complete_file_path(logs_dir):
    # A way for checking if the keeper DB is complete without inspecting the DB
    # file. This can be used to inform connection decisions, like if the DB
    # is not expected to change.
    return os.path.join(get_db_file_dir_path(logs_dir), ".keeper_complete")


def is_keeper_db_complete(logs_dir):
    return os.path.isfile(get_keeper_complete_file_path(logs_dir))


def get_keeper_query_ready_file_path(logs_dir: str) -> str:
    return os.path.join(get_db_file_dir_path(logs_dir), ".keeper_query_ready")


def is_keeper_db_query_ready(logs_dir: str) -> bool:
    return os.path.isfile(get_keeper_query_ready_file_path(logs_dir))


def get_db_file(logs_dir: str, new=False) -> str:
    db_file = os.path.join(get_db_file_dir_path(logs_dir), "firex_run.db")
    if new:
        assert not os.path.exists(db_file), (
            f"Cannot create new DB file, it already exists: {db_file}"
        )
        db_file_parent = os.path.dirname(db_file)
        os.makedirs(db_file_parent, exist_ok=True)
    else:
        assert os.path.isfile(db_file), f"DB file does not exist: {db_file}"
    return db_file


@contextmanager
def get_db_manager(logs_dir):
    "Get a query-only DB manager for an existing keeper DB file."

    conn = connect_db(
        get_db_file(logs_dir),
        read_only=True,
        is_run_complete=is_keeper_db_complete(logs_dir),
    )
    db_manager = FireXRunDbManager(conn)
    try:
        yield db_manager
    finally:
        db_manager.close()


def _row_to_run_metadata(row):
    # By column name, so fields missing from the table (e.g. firex_requester) are None.
    row_by_col = row._mapping
    return FireXRunMetadata(**{f: row_by_col.get(f) for f in FireXRunMetadata._fields})


RETRYING_DB_EXCEPTIONS = (OperationalError, SqlLiteOperationalError)
DEFAULT_MAX_RETRY_ATTEMPTS = 20


def retry(
    exceptions, max_attempts: int = DEFAULT_MAX_RETRY_ATTEMPTS, retry_delay: int = 1
):
    def retry_decorator(func):
        def retrying_wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if attempt >= max_attempts:
                        raise
                    sleep(retry_delay)

        return retrying_wrapper

    return retry_decorator


class FireXRunDbManager:
    """
    Read-only operations on a keeper DB connection.
    """

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def does_task_whereclause_exist(self, whereclause):
        query = select(firex_tasks.c.uuid).where(whereclause)
        return self.db_conn.execute(query).scalar() is not None

    def wait_before_query(self, whereclause, max_wait, error_on_wait_exceeded):
        if not self._is_keeper_complete():
            start_wait_time = perf_counter()
            # Each check must be its own read transaction: one held across checks would never see
            # newly written tasks and would block the writer's commits for the whole wait.
            exists = wait_until(
                self.does_task_whereclause_exist, max_wait, 0.5, whereclause
            )
            if not exists:
                msg = f"Wait exceeded {max_wait} seconds for {whereclause} to exist, but it still does not."
                if error_on_wait_exceeded:
                    raise FireXWaitQueryExceeded(msg)
                else:
                    logger.warning(msg)
            else:
                wait_duration = perf_counter() - start_wait_time
                logger.debug(
                    f"Keeper query waited {wait_duration:.2f} secs for wait query to exist."
                )

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_tasks(
        self, exp, wait_for_exp_exist=None, max_wait=15, error_on_wait_exceeded=False
    ) -> list[FireXTask]:
        if wait_for_exp_exist is not None:
            self.wait_before_query(wait_for_exp_exist, max_wait, error_on_wait_exceeded)

        if isinstance(exp, Select):
            select_stmt = exp
        else:
            select_stmt = select(firex_tasks).where(exp)

        db_result = self.db_conn.execute(select_stmt)
        result_tasks = []
        for row in db_result:
            try:
                result_tasks.append(FireXTask(*row))
            except TypeError:
                logger.exception("Failed transforming %s", row[0])
                raise
        return result_tasks

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_run_metadata(self, firex_id) -> FireXRunMetadata:
        result = self.db_conn.execute(
            select(firex_run_metadata).where(firex_run_metadata.c.firex_id == firex_id)
        )
        if not result:
            raise LookupError(f"Found no run data for {firex_id}")
        return next(_row_to_run_metadata(row) for row in result)

    def _query_single_run_metadata_row(self):
        result = self.db_conn.execute(select(firex_run_metadata))
        rows = [r for r in result]
        if len(rows) != 1:
            raise RuntimeError(
                f"Expected exactly one firex_run_metadata, but found {len(rows)}"
            )
        return rows[0]

    @retry(RETRYING_DB_EXCEPTIONS)
    def _is_keeper_complete(self) -> bool:
        return self._query_single_run_metadata_row()["keeper_complete"]

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_single_run_metadata(self) -> FireXRunMetadata:
        return _row_to_run_metadata(self._query_single_run_metadata_row())

    def close(self):
        self.db_conn.close()
