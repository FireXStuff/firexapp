import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager
from multiprocessing import Process, Queue
from sqlite3 import OperationalError as SqlLiteOperationalError
from unittest.mock import patch

from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError

from firex_keeper import task_query
from firex_keeper.db_model import firex_tasks, task_skuid_exp
from firex_keeper.keeper_event_consumer import (
    KeeperThreadedEventWriter,
    WritingFireXRunDbManager,
)
from firex_keeper.keeper_helper import can_any_write
from firex_keeper.persist import (
    DEFAULT_MAX_RETRY_ATTEMPTS,
    KEEPER_PAGE_SIZE,
    FireXWaitQueryExceeded,
    get_db_file,
    get_db_manager,
    task_by_uuid_exp,
    task_uuid_complete_exp,
)
from firexapp.events.model import FireXRunMetadata, RunStates
from firexkit.result import ChainInterruptedException


def __write_events(logs_dir, events):
    event_writer = KeeperThreadedEventWriter(
        FireXRunMetadata("FireX-1", logs_dir, "Noop", None, firex_requester=None),
        lambda r: None,
    )

    for e in events:
        try:
            event_writer.queue_celery_event(e)
        except Exception:
            # do not hang test running, this is test writing error.
            event_writer.stop()
            raise

    return event_writer


def _write_and_wait_for_stop(logs_dir, events, q):
    event_writer = __write_events(logs_dir, events)

    while True:
        maybe_event = q.get()  # block till something on the queue
        if maybe_event is None:
            event_writer.stop()
            break
        else:
            event_writer.queue_celery_event(maybe_event)


class ProcessedKeeperWriter:
    def __init__(self, proc, q):
        self.p = proc
        self.q = q

    def stop(self):
        self.q.put(None)
        self.p.join()

    def queue_event(self, event):
        self.q.put(event)


def _write_events_to_db(logs_dir, events, stop=True):
    if stop:
        writer = __write_events(logs_dir, events)
        writer.stop()
        return None
    else:
        # can't have writing/querying in same process from different threads,
        # so run data writing in seperate process for tests that want to query
        # "in-progress" keeper DBs.
        q = Queue()
        p = Process(target=_write_and_wait_for_stop, args=(logs_dir, events, q))
        p.start()
        task_query.wait_on_keeper_query_ready(logs_dir, timeout=5)
        return ProcessedKeeperWriter(p, q)


def chain_exception_str(uuid):
    return ChainInterruptedException(task_id=uuid).__repr__()


tree_events = [
    #           1
    #       2       3
    #              4 5
    {
        "uuid": "1",
        "name": "Root",
        "parent_id": None,
        "exception": chain_exception_str("3"),
    },
    {"uuid": "2", "name": "Noop", "parent_id": "1"},
    {
        "uuid": "3",
        "name": "Noop",
        "parent_id": "1",
        "exception": chain_exception_str("4"),
    },
    {
        "uuid": "4",
        "name": "Noop",
        "parent_id": "3",
        "exception": "SomeNonChainException()",
    },
    {"uuid": "5", "name": "Noop", "parent_id": "3"},
]


class FireXKeeperTests(unittest.TestCase):
    def test_query_by_name_and_uuid(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop"},
                    {"uuid": "2", "name": "Noop"},
                    {"uuid": "3", "name": "Other"},
                ],
            )

            tasks = task_query.tasks_by_name(logs_dir, "Noop")
            self.assertEqual(2, len(tasks))
            self.assertEqual("Noop", tasks[0].name)

            other_task = task_query.task_by_uuid(logs_dir, "3")
            self.assertEqual("Other", other_task.name)

    def test_query_copy_db(self):
        with tempfile.TemporaryDirectory() as logs_dir:
            _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop"},
                    {"uuid": "2", "name": "Noop"},
                    {"uuid": "3", "name": "Other"},
                ],
            )

            tasks = task_query.tasks_by_name(logs_dir, "Noop", copy_before_query=True)
            self.assertEqual(2, len(tasks))

    def test_query_all(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop"},
                    {"uuid": "2", "name": "Other"},
                    {"uuid": "3", "name": "Third"},
                ],
            )
            self.assertEqual(3, len(task_query.all_tasks(logs_dir)))

    def test_query_failure_after_update(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop"},
                    {"uuid": "2", "name": "Noop"},
                    {"uuid": "1", "type": RunStates.FAILED.value},
                ],
            )

            tasks = task_query.failed_tasks(logs_dir)
            self.assertEqual(1, len(tasks))
            self.assertEqual(RunStates.FAILED.value, tasks[0].state)

    def test_query_tree(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tree = task_query._create_task_tree(logs_dir, root_uuid="3")
            child_uuids = {t.uuid for t in tree.children}
            self.assertEqual({"4", "5"}, child_uuids)

    def test_query_tree_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tree = task_query._create_task_tree(logs_dir, root_uuid="not exists")
            self.assertIsNone(tree)

    def test_get_decendants(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tasks = task_query.get_descendants(logs_dir, "3")
            child_uuids = {t.uuid for t in tasks}
            self.assertEqual({"4", "5"}, child_uuids)

    def test_get_decendants_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tasks = task_query.get_descendants(logs_dir, "not found UUID")
            self.assertEqual([], tasks)

    def test_find_task_causing_chain_exception(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tree = task_query._create_task_tree(logs_dir)
            causing_task = task_query.find_task_causing_chain_exception(tree)
            self.assertEqual("4", causing_task.uuid)

    def test_wait_for_task_exist(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop"},
                    {"uuid": "2", "name": "Noop"},
                    {"uuid": "3", "name": "Other"},
                ],
            )
            tasks = task_query.tasks_by_name(
                logs_dir,
                "Noop",
                wait_for_exp_exist=task_by_uuid_exp("3"),
                max_wait=1,
                error_on_wait_exceeded=True,
            )
            self.assertEqual(2, len(tasks))

    def test_wait_for_task_not_exist(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            stopper = _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop"},
                    {"uuid": "2", "name": "Noop"},
                    {"uuid": "3", "name": "Other"},
                ],
                stop=False,
            )

            self.assertRaises(
                FireXWaitQueryExceeded,
                task_query.tasks_by_name,
                logs_dir,
                "Noop",
                wait_for_exp_exist=task_by_uuid_exp("some fake uuid"),
                max_wait=1,
                error_on_wait_exceeded=True,
            )
            stopper.stop()

    def test_wait_for_task_complete(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)

            stopper = _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "Noop", "type": RunStates.STARTED.value},
                    {"uuid": "2", "name": "Noop"},
                ],
                stop=False,
            )
            self.assertTrue(can_any_write(get_db_file(logs_dir)))
            # Make sure that task 1 is not yet complete
            self.assertRaises(
                FireXWaitQueryExceeded,
                task_query.tasks_by_name,
                logs_dir,
                "Noop",
                wait_for_exp_exist=task_uuid_complete_exp("1"),
                max_wait=1,
                error_on_wait_exceeded=True,
            )

            stopper.stop()

            # expect result now that the task is complete, not an exception.
            tasks = task_query.tasks_by_name(
                logs_dir,
                "Noop",
                wait_for_exp_exist=task_uuid_complete_exp("1"),
                max_wait=3,
                error_on_wait_exceeded=True,
            )
            self.assertEqual(2, len(tasks))

            # Verify that after a run is complete, the DB is no longer writeable.
            # TODO: Enable after verifying this doesn't harm cleanup.
            # self.assertFalse(can_any_write(get_db_file(logs_dir)))

    def test_int_in_json_column(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(
                logs_dir, [{"uuid": "1", "name": "Noop", "firex_result": 0}]
            )

            task = task_query.single_task_by_name(logs_dir, "Noop")
            self.assertEqual(0, task.firex_result)

    def test_query_running(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            stopper = _write_events_to_db(
                logs_dir,
                [
                    {"uuid": "1", "name": "run1", "type": RunStates.STARTED.value},
                    {"uuid": "2", "name": "run2", "type": RunStates.UNBLOCKED.value},
                    {
                        "uuid": "3",
                        "name": "done",
                        "firex_result": 0,
                        "type": RunStates.FAILED.value,
                    },
                ],
                stop=False,
            )

            tasks = task_query.running_tasks(
                logs_dir,
                wait_for_exp_exist=task_by_uuid_exp("3"),
                max_wait=2,
                error_on_wait_exceeded=True,
            )
            self.assertEqual(2, len(tasks))

            stopper.stop()

            # Stopping keeper sets all incomplete tasks to a non-celery
            # terminal run state.
            tasks = task_query.running_tasks(logs_dir)
            self.assertEqual(0, len(tasks))

    def test_query_by_failed_by(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(
                logs_dir,
                [
                    {
                        "uuid": "1",
                        "name": "Noop",
                    },
                    {"uuid": "2", "name": "FailedByChild", "exception_cause_uuid": "3"},
                    {"uuid": "3", "name": "Fail", "type": RunStates.FAILED.value},
                ],
            )

            tasks = task_query.failed_by_tasks(logs_dir, "3")
            self.assertEqual(1, len(tasks))
            self.assertEqual("FailedByChild", tasks[0].name)
            self.assertEqual("2", tasks[0].uuid)

    def test_event_after_task_completed(self):
        """
        Receiving an event for a completed task is a special case
        in the aggregator since the task record needs to be read
        from the DB. This is because completed tasks are removed
        from memory to keep memory use lower.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)

            writer_proc = _write_events_to_db(
                logs_dir,
                [
                    # an incomplete root keeps the keeper writing.
                    {
                        "uuid": "0",
                        "name": "Root",
                        "parent_id": None,
                        "type": RunStates.RECEIVED.value,
                    },
                    {
                        "uuid": "1",
                        "name": "Noop",
                        "parent_id": "0",
                        "type": RunStates.FAILED.value,
                    },
                ],
                stop=False,
            )

            # Make sure that task 1 is not yet complete
            tasks = task_query.tasks_by_name(
                logs_dir,
                "Noop",
                wait_for_exp_exist=task_by_uuid_exp("1"),
                max_wait=2,
                error_on_wait_exceeded=True,
            )
            self.assertEqual(1, len(tasks))

            writer_proc.queue_event(
                {"uuid": "1", "type": RunStates.STARTED.value},
            )

            tasks = task_query.tasks_by_name(
                logs_dir,
                "Noop",
                wait_for_exp_exist=firex_tasks.c.state == RunStates.STARTED.value,
                max_wait=4,
                error_on_wait_exceeded=True,
            )
            self.assertEqual(1, len(tasks))
            self.assertEqual(RunStates.STARTED.value, tasks[0].state)

            writer_proc.stop()

            # expect result now that the task is complete, not an exception.
            tasks = task_query.tasks_by_name(logs_dir, "Noop")
            self.assertEqual(1, len(tasks))

    def test_write_retry_success(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_writer = _create_writing_db_mgr(tmpdirname)
            unmocked_insert_task = db_writer.insert_task
            with patch.object(
                db_writer, "insert_task", wraps=db_writer.insert_task
            ) as mock_insert:
                # fail three, then succeed, relying on retries.
                def _side_effect(*args, **kwargs):
                    if mock_insert.call_count < 3:
                        raise OperationalError("DB failed", params={}, orig=None)
                    return unmocked_insert_task(*args, **kwargs)

                mock_insert.side_effect = _side_effect

                db_writer.aggregate_events_and_update_db(
                    [{"uuid": "1", "name": "Noop"}]
                )
                self.assertEqual(mock_insert.call_count, 3)

    def test_write_retry_success_sqlliteoperationalerror(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_writer = _create_writing_db_mgr(tmpdirname)
            unmocked_insert_task = db_writer.insert_task
            with patch.object(
                db_writer, "insert_task", wraps=db_writer.insert_task
            ) as mock_insert:
                # fail three, then succeed, relying on retries.
                def _side_effect(*args, **kwargs):
                    if mock_insert.call_count < 3:
                        raise SqlLiteOperationalError("database is locked")
                    return unmocked_insert_task(*args, **kwargs)

                mock_insert.side_effect = _side_effect

                db_writer.aggregate_events_and_update_db(
                    [{"uuid": "1", "name": "Noop"}]
                )
                self.assertEqual(mock_insert.call_count, 3)

    def test_write_retry_fail(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_writer = _create_writing_db_mgr(tmpdirname)
            with patch.object(db_writer, "insert_task") as mock_insert:
                # fail more times than we'll retry for.
                mock_insert.side_effect = [
                    OperationalError("DB failed", params={}, orig=None)
                    for _ in range(DEFAULT_MAX_RETRY_ATTEMPTS + 1)
                ]
                db_writer.aggregate_events_and_update_db(
                    [{"uuid": "1", "name": "Noop"}]
                )
                self.assertEqual(mock_insert.call_count, DEFAULT_MAX_RETRY_ATTEMPTS)
                self.assertEqual(db_writer.query_tasks(True), [])

    def test_write_retry_fail_sqlliteoperationalerror(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_writer = _create_writing_db_mgr(tmpdirname)
            with patch.object(db_writer, "insert_task") as mock_insert:
                # fail more times than we'll retry for.
                mock_insert.side_effect = [
                    SqlLiteOperationalError("database is locked")
                    for _ in range(DEFAULT_MAX_RETRY_ATTEMPTS + 1)
                ]
                db_writer.aggregate_events_and_update_db(
                    [{"uuid": "1", "name": "Noop"}]
                )
                self.assertEqual(mock_insert.call_count, DEFAULT_MAX_RETRY_ATTEMPTS)
                self.assertEqual(db_writer.query_tasks(True), [])

    def test_first_started_kept_across_writes(self):
        """
        A task created in one write keeps first_started when its next event
        arrives in a later write, e.g. its completion after a long run.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_writer = _create_writing_db_mgr(tmpdirname)
            db_writer.aggregate_events_and_update_db(
                [
                    {
                        "uuid": "1",
                        "name": "Noop",
                        "type": RunStates.RECEIVED.value,
                        "local_received": 100.0,
                    },
                    {
                        "uuid": "1",
                        "type": RunStates.STARTED.value,
                        "local_received": 100.5,
                    },
                ]
            )
            db_writer.aggregate_events_and_update_db(
                [
                    {
                        "uuid": "1",
                        "type": RunStates.SUCCEEDED.value,
                        "local_received": 7300.0,
                    }
                ]
            )
            tasks = db_writer.query_tasks(True)
            self.assertEqual(1, len(tasks))
            self.assertEqual(100.0, tasks[0].first_started)
            self.assertEqual(RunStates.SUCCEEDED.value, tasks[0].state)


def _create_writing_db_mgr(logs_dir: str) -> WritingFireXRunDbManager:
    return WritingFireXRunDbManager(
        FireXRunMetadata("FireX-1", logs_dir, "SomeChain", None, "username")
    )


def _pragma(db_conn, name):
    return db_conn.execute(text(f"PRAGMA {name}")).scalar()


def test_keeper_db_pragmas(tmp_path):
    logs_dir = str(tmp_path)
    db_writer = _create_writing_db_mgr(logs_dir)
    try:
        db_writer.aggregate_events_and_update_db([{"uuid": "1", "name": "Noop"}])
        assert _pragma(db_writer.db_conn, "journal_mode") == "truncate"
        assert _pragma(db_writer.db_conn, "busy_timeout") == 60000

        with get_db_manager(logs_dir) as db_manager:
            assert _pragma(db_manager.db_conn, "page_size") == KEEPER_PAGE_SIZE
            assert _pragma(db_manager.db_conn, "query_only") == 1
    finally:
        db_writer.complete_writing()


def test_query_copy_db_without_rsync(tmp_path, monkeypatch):
    logs_dir = str(tmp_path)
    _write_events_to_db(
        logs_dir,
        [{"uuid": "1", "name": "Noop"}, {"uuid": "2", "name": "Other"}],
    )
    monkeypatch.setattr(task_query.shutil, "which", lambda _: None)
    with patch.object(
        task_query.shutil,
        "copyfile",
        side_effect=AssertionError("expected sqlite backup"),
    ):
        tasks = task_query.tasks_by_name(
            logs_dir, "Noop", copy_before_query=True, db_file_query_ready_timeout=0
        )
    assert [t.uuid for t in tasks] == ["1"]


def test_copy_db_leaves_hot_looking_journal(tmp_path, monkeypatch):
    # A host that can't see the writer's NFS lock sees the live DB with a journal that looks hot.
    # Rolling that journal back would corrupt the live DB, so copying must not.
    writing_db = tmp_path / "writing.db"
    writer = sqlite3.connect(writing_db, isolation_level=None)
    writer.execute("CREATE TABLE t (v TEXT)")
    writer.execute("BEGIN")
    writer.executemany("INSERT INTO t VALUES (?)", [("old" * 100,)] * 2000)
    writer.execute("COMMIT")
    # spill the uncommitted update to the DB file.
    writer.execute("PRAGMA cache_size=2")
    writer.execute("BEGIN")
    writer.execute("UPDATE t SET v = 'new'")
    seen_db = tmp_path / "seen.db"
    for suffix in ("", "-journal"):
        shutil.copyfile(f"{writing_db}{suffix}", f"{seen_db}{suffix}")
    writer.close()
    seen_db_bytes = seen_db.read_bytes()

    monkeypatch.setattr(task_query.shutil, "which", lambda _: None)
    task_query._copy_keeper_db_for_local_query(
        str(seen_db), str(tmp_path / "copy.db"), str(tmp_path)
    )

    assert seen_db.read_bytes() == seen_db_bytes
    assert (tmp_path / "seen.db-journal").exists()


@contextmanager
def _reader_mid_query(db_file):
    # A reader part way through a query holds its read lock, so the writer can't commit.
    with closing(sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)) as reader:
        cursor = reader.execute("SELECT uuid FROM firex_tasks")
        cursor.fetchone()
        yield
        cursor.fetchall()


def _new_reader_result(db_file):
    with closing(sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=0)) as r:
        try:
            return sorted(u for (u,) in r.execute("SELECT uuid FROM firex_tasks"))
        except sqlite3.OperationalError as e:
            return str(e)


def test_events_not_written_while_locked_are_written_later(tmp_path, monkeypatch):
    logs_dir = str(tmp_path)
    db_writer = _create_writing_db_mgr(logs_dir)
    db_writer.db_conn.execute(text("PRAGMA busy_timeout=0"))  # fail as soon as locked.
    db_writer.aggregate_events_and_update_db(
        [{"uuid": "1", "name": "Noop"}, {"uuid": "2", "name": "Noop"}]
    )
    db_file = get_db_file(logs_dir)
    # check new readers while the writer waits to retry COMMIT, instead of sleeping.
    readers_during_commit_retries = []
    monkeypatch.setattr(
        "firex_keeper.keeper_event_consumer.sleep",
        lambda _: readers_during_commit_retries.append(_new_reader_result(db_file)),
    )

    with _reader_mid_query(db_file):
        db_writer.aggregate_events_and_update_db([{"uuid": "3", "name": "Noop"}])
        # once the writer gives up, it rolls back and stops blocking new readers.
        assert _new_reader_result(db_file) == ["1", "2"]
    assert readers_during_commit_retries == ["database is locked"] * (
        DEFAULT_MAX_RETRY_ATTEMPTS - 1
    )

    db_writer.aggregate_events_and_update_db([{"uuid": "4", "name": "Noop"}])
    assert _new_reader_result(db_file) == ["1", "2", "3", "4"]
    # unwritten events were aggregated again from scratch, so task "3" is fully tracked.
    assert {e["uuid"] for e in db_writer.generate_incomplete_events()} == {
        "1",
        "2",
        "3",
        "4",
    }

    with _reader_mid_query(db_file):
        db_writer.aggregate_events_and_update_db([{"uuid": "5", "name": "Noop"}])
    db_writer.complete_writing()
    with get_db_manager(logs_dir) as db_manager:
        assert [t.uuid for t in db_manager.query_tasks(task_by_uuid_exp("5"))] == ["5"]


def test_find_task_by_skuid_uses_index(tmp_path):
    db_writer = _create_writing_db_mgr(str(tmp_path))
    try:
        db_writer.aggregate_events_and_update_db(
            [
                {
                    "uuid": "1",
                    "name": "BuildAndRegress",
                    "firex_bound_args": {"skuid": "a"},
                },
                # SQLite's JSON functions reject NaN, which mustn't stop the task's write.
                {
                    "uuid": "2",
                    "name": "BuildAndRegress",
                    "firex_bound_args": {"skuid": "b", "x": float("nan")},
                },
                {"uuid": "3", "name": "Noop"},
            ]
        )
        assert sorted(t.uuid for t in db_writer.query_tasks(True)) == ["1", "2", "3"]

        query = select(firex_tasks).where(task_skuid_exp == "a")
        assert [t.uuid for t in db_writer.query_tasks(query)] == ["1"]
        compiled = query.compile(dialect=db_writer.db_conn.dialect)
        plan = db_writer.db_conn.exec_driver_sql(
            f"EXPLAIN QUERY PLAN {compiled}",
            tuple(compiled.params[p] for p in compiled.positiontup),
        ).fetchall()
        assert "USING INDEX task_skuid_index" in str(plan)
    finally:
        db_writer.complete_writing()
