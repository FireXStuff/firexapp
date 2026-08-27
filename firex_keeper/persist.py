import json
import logging
import os
from contextlib import contextmanager
from time import perf_counter, sleep

from firexapp.submit.uid import Uid
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_
from sqlalchemy.sql.selectable import Select
from sqlalchemy.exc import OperationalError
from sqlite3 import OperationalError as SqlLiteOperationalError

from firexapp.events.model import FireXTask, FireXRunMetadata, COMPLETE_RUNSTATES
from firexapp.common import wait_until
from firex_keeper.db_model import metadata, firex_run_metadata, firex_tasks

logger = logging.getLogger(__name__)


class FireXWaitQueryExceeded(Exception):
    pass


def task_by_uuid_exp(task_uuid):
    return firex_tasks.c.uuid == task_uuid


def task_uuid_complete_exp(task_uuid):
    return and_(firex_tasks.c.uuid == task_uuid,
                firex_tasks.c.state.in_(COMPLETE_RUNSTATES))


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


def _get_pragmas(use_wal):
    pragmas = [ 'page_size = 4096' ]
    if use_wal:
        pragmas += [
            'journal_mode=WAL', 'synchronous=NORMAL',
        ]
    return pragmas


def execute_pragmas(engine, use_wal=False):
    dbapi_connection = engine.raw_connection()
    try:
        cursor = dbapi_connection.cursor()
        for pragma in _get_pragmas(use_wal):
            cmd = 'PRAGMA ' + pragma
            logger.debug(f"Executing: {cmd}")
            cursor.execute(cmd)
        cursor.close()
        dbapi_connection.commit()
    finally:
        dbapi_connection.close()


def _db_connection_str(db_file, read_only, is_run_complete=False):
    db_conn_str = f'sqlite:///file:{db_file}'

    params = {'uri': 'true'}

    if is_run_complete:
        params['immutable'] = '1'
        read_only = True

    if read_only:
        params['mode'] = 'ro'

    db_conn_str += '?' + '&'.join(
        [f'{k}={v}' for k, v in params.items()]
    )

    return db_conn_str


def connect_db(db_file, read_only=False, metadata_to_create=metadata, is_run_complete=False):
    engine = create_engine(
        _db_connection_str(db_file, read_only, is_run_complete=is_run_complete),
        json_deserializer=_custom_json_loads,
    )

    if not os.path.exists(db_file):
        #  WAL should not be used while concurrent read+write NFS access is still possible. Once all reads go through
        #   keeper process for in-progress runs, WAL is likely preferable for in-progress runs, then after the DB
        #   should be read-only and therefore safe for direct NFS access.
        execute_pragmas(engine, use_wal=False)

        logger.info("Creating schema for %s" % db_file)
        metadata_to_create.create_all(engine)
        logger.info("Schema creation complete for %s" % db_file)

    return engine.connect()


def get_db_file_dir_path(logs_dir: str) -> str:
    return os.path.join(logs_dir, Uid.debug_dirname, 'keeper')


def get_keeper_complete_file_path(logs_dir):
    # A way for checking if the keeper DB is complete without inspecting the DB
    # file. This can be used to inform connection decisions, like if the DB
    # is not expected to change.
    return os.path.join(get_db_file_dir_path(logs_dir), '.keeper_complete')


def is_keeper_db_complete(logs_dir):
    return os.path.isfile(get_keeper_complete_file_path(logs_dir))


def get_keeper_query_ready_file_path(logs_dir: str) -> str:
    return os.path.join(get_db_file_dir_path(logs_dir), '.keeper_query_ready')


def is_keeper_db_query_ready(logs_dir: str) -> bool:
    return os.path.isfile(
        get_keeper_query_ready_file_path(logs_dir)
    )


def get_db_file(logs_dir: str, new=False) -> str:
    db_file = os.path.join(get_db_file_dir_path(logs_dir), 'firex_run.db')
    if new:
        assert not os.path.exists(db_file), f"Cannot create new DB file, it already exists: {db_file}"
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
    # The first 4 columns from the table make up a FireXRunMetadata.
    # Need to set firex_requester explicitly since it's not in the db_mode.firex_run_metadata
    # Can't add the Column now as it won't be backward compatible
    return FireXRunMetadata(*row[:4], firex_requester=None)

RETRYING_DB_EXCEPTIONS = (OperationalError, SqlLiteOperationalError)
DEFAULT_MAX_RETRY_ATTEMPTS = 20

def retry(exceptions, max_attempts: int=DEFAULT_MAX_RETRY_ATTEMPTS, retry_delay: int=1):
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
        query = select([firex_tasks.c.uuid]).where(whereclause)
        return self.db_conn.execute(query).scalar() is not None

    def wait_before_query(self, whereclause, max_wait, error_on_wait_exceeded):
        if not self._is_keeper_complete():
            start_wait_time = perf_counter()
            exists = wait_until(self.does_task_whereclause_exist, max_wait, 0.5, whereclause)
            if not exists:
                msg = f"Wait exceeded {max_wait} seconds for {whereclause} to exist, but it still does not."
                if error_on_wait_exceeded:
                    raise FireXWaitQueryExceeded(msg)
                else:
                    logger.warning(msg)
            else:
                wait_duration = perf_counter() - start_wait_time
                logger.debug(f"Keeper query waited {wait_duration:.2f} secs for wait query to exist.")

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_tasks(self, exp, wait_for_exp_exist=None, max_wait=15, error_on_wait_exceeded=False) -> list[FireXTask]:
        if wait_for_exp_exist is not None:
            self.wait_before_query(wait_for_exp_exist, max_wait, error_on_wait_exceeded)

        if isinstance(exp, Select):
            select_stmt = exp
        else:
            select_stmt = select([firex_tasks]).where(exp)

        db_result = self.db_conn.execute(select_stmt)
        result_tasks = []
        for row in db_result:
            try:
                result_tasks.append(FireXTask(*row))
            except TypeError as e:
                logger.error(f"Failed transforming {row[0]}")
                logger.exception(e)
                raise
        return result_tasks

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_run_metadata(self, firex_id) -> FireXRunMetadata:
        result = self.db_conn.execute(select([firex_run_metadata]).where(firex_run_metadata.c.firex_id == firex_id))
        if not result:
            raise Exception(f"Found no run data for {firex_id}")
        return [_row_to_run_metadata(row) for row in result][0]

    def _query_single_run_metadata_row(self):
        result = self.db_conn.execute(select([firex_run_metadata]))
        rows = [r for r in result]
        if len(rows) != 1:
            raise Exception(f"Expected exactly one firex_run_metadata, but found {len(rows)}")
        return rows[0]

    @retry(RETRYING_DB_EXCEPTIONS)
    def _is_keeper_complete(self) -> bool:
        return self._query_single_run_metadata_row()['keeper_complete']

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_single_run_metadata(self) -> FireXRunMetadata:
        return _row_to_run_metadata(self._query_single_run_metadata_row())

    def close(self):
        self.db_conn.close()

