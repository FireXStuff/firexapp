"""
Process events from Celery.
"""

from enum import Enum, auto
import logging
import queue
import threading
from time import sleep
from pathlib import Path
from typing import Optional, Any

from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.event_aggregator import DEFAULT_AGGREGATOR_CONFIG, AbstractFireXEventAggregator
from firexapp.events.model import FireXRunMetadata, get_task_data, RunMetadataColumn
import sqlalchemy.exc
from sqlite3 import DatabaseError as SqlLiteDatabaseError
from firexapp.events.model import RunStates

from firex_keeper.db_model import firex_run_metadata, firex_tasks
from firex_keeper.persist import (get_keeper_complete_file_path,
    task_by_uuid_exp, FireXRunDbManager, get_keeper_query_ready_file_path,
    RETRYING_DB_EXCEPTIONS, retry, connect_db, get_db_file,
)


logger = logging.getLogger(__name__)


class KeeperQueueEntryType(Enum):
    CELERY_EVENT = auto()
    STOP = auto()


def _drain_queue(q):
    items = []
    for _ in range(q.qsize()):
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            pass
    return items


class KeeperThreadedEventWriter:
    """
        Aggregates Celery events from a queue and writes to the Keeper DB
        in a seperate thread.
    """

    def __init__(self, run_metadata, writing_complete_callback):
        # The writer thread exclusively connects to the DB, so it creates the aggregating_writer.
        # This class (and its thread) expose a subset of aggregated data that DOES NOT rely on querying the DB.
        # It's important only one thread accesses the DB to avoid data corruption.
        self._aggregating_writer = None

        # track if the writer thread has fully failed so we can fail early instead of queuing Celery
        # events in memory indefinitely.
        self._writer_fully_failed : bool = False

        self.celery_event_queue = queue.Queue()
        self._writing_complete_callback = writing_complete_callback
        self._writing_thread = threading.Thread(target=self._write_events_from_queue, args=(run_metadata,))
        self._writing_thread.start()

    def _write_events_from_queue(self, run_metadata, sleep_after_events=2):
        completion_reason = ''
        try:
            self._aggregating_writer = WritingFireXRunDbManager(run_metadata)
            self._aggregating_writer.insert_run_metadata(run_metadata)
            while True:
                # wait indefinitely for next item, either celery event or "stop" control signal.
                queue_item = self.celery_event_queue.get()

                # drain queue to group events in to single DB write.
                queue_items = [queue_item] + _drain_queue(self.celery_event_queue)

                celery_events = [i[1] for i in queue_items if i[0] == KeeperQueueEntryType.CELERY_EVENT]
                if celery_events:
                    self._aggregating_writer.aggregate_events_and_update_db(celery_events)
                    for _ in celery_events:
                        self.celery_event_queue.task_done()

                stop = [i for i in queue_items if i[0] == KeeperQueueEntryType.STOP]
                if stop or self.are_all_tasks_complete():
                    if stop:
                        completion_reason = 'Celery event receiving is complete.'
                    else:
                        completion_reason = 'all tasks complete.'
                    for _ in stop:
                        self.celery_event_queue.task_done()
                    break

                # Sleep to allow events to accumulate so that writes are grouped.
                sleep(sleep_after_events)
        except:
            logger.exception('Failed processing write queue entries; will stop event receiving.')
            completion_reason = 'failure while writing events.'
            raise # nowhere to go since this is expected to be the top of a thread.
        finally:
            if self._aggregating_writer:
                self._aggregating_writer.complete_writing()
            self._writing_complete_callback(completion_reason)

    def is_root_complete(self) -> bool:
        # This method is (and must be) threadsafe and not access the DB.
        return bool(
            self._aggregating_writer
            and self._aggregating_writer.is_root_complete()
        )

    def are_all_tasks_complete(self):
        if not self.is_root_complete():
            return False
        # This method is (and must be) threadsafe and not access the DB.
        return bool(
            self._aggregating_writer
            and self._aggregating_writer.are_all_tasks_complete())

    def queue_celery_event(self, celery_event):
        self.celery_event_queue.put(
            (KeeperQueueEntryType.CELERY_EVENT, celery_event),
        )

    def stop(self):
        self.celery_event_queue.put(
            (KeeperQueueEntryType.STOP, None),
        )
        self._writing_thread.join()


class KeeperEventAggregator(AbstractFireXEventAggregator):
    """
        Aggregates many events in to the task data model.
        Tries to minimize memory usage and disk-reads by
        keeping only incomplete tasks in memory. Task
        completeness is ill defined, so it may always be
        necessary to read a task in order to perform
        aggregation.
    """

    def __init__(self, run_db_manager: 'WritingFireXRunDbManager', firex_id: str):
        super().__init__(DEFAULT_AGGREGATOR_CONFIG)
        self.run_db_manager : WritingFireXRunDbManager = run_db_manager
        self.firex_id = firex_id

        # All task UUIDs stored, but only incomplete
        # tasks kept here (None for complete task UUIDs).
        # This minimizes memory usage.
        self.maybe_tasks_by_uuid : dict[str, Optional[dict]] = {}
        self.root_task_uuid = None

    def aggregate_events(self, events):
        self._maybe_set_root_uuid(events)
        return super().aggregate_events(events)

    def update_in_memory_tasks(self, new_data_by_task_uuid):
        # Update in-memory task tracking after aggregate_events in case DB
        # writes fail.
        for uuid, new_task_data in new_data_by_task_uuid.items():
            if uuid not in self.maybe_tasks_by_uuid:
                self.maybe_tasks_by_uuid[uuid] = {} # new task

            if self.maybe_tasks_by_uuid[uuid] is None:
                # event for complete task, reload & update. Note
                # the task state scheme is crazy since task-failure may or
                # may not be terminal due to retries.
                self.maybe_tasks_by_uuid[uuid] = self._get_task(uuid)

            if self.maybe_tasks_by_uuid[uuid] is not None:
                # incomplete task updated.
                self.maybe_tasks_by_uuid[uuid].update(new_task_data)

                # check if this update put the task in to a complete runstate.
                if RunStates.is_complete_state(
                    self.maybe_tasks_by_uuid[uuid].get('state')
                ):
                    # None means the task is complete, reducing memory footprint
                    # If new events are received for this task, it will be loaded
                    # from the DB via self._get_task
                    self.maybe_tasks_by_uuid[uuid] = None

    def is_root_complete(self) -> bool:
        # Need to override this to avoid accessing DB from broker processor thread
        # since base class accesses root task via _get_task. Can't access DB across threads.
        return bool(
            self.root_task_uuid is not None
            and self.root_task_uuid in self.maybe_tasks_by_uuid
            # None tasks mean complete
            and self.maybe_tasks_by_uuid[self.root_task_uuid] is None
        )

    def _maybe_set_root_uuid(self, events):
        if self.root_task_uuid is not None:
            return # root already set by previous event.

        self.root_task_uuid = next(
            (
                e.get('root_id') for e in events
                if e.get('type') == 'task-received' and e.get('root_id')
            ),
            None
        )
        if not self.root_task_uuid:
            self.root_task_uuid = next(
                (e.get('uuid') for e in events
                if e.get('parent_id') is None and e.get('uuid')),
                None
            )
        if not self.root_task_uuid:
            self.root_task_uuid = next(
                (e.get('uuid') for e in events),
                None
            )

        if self.root_task_uuid:
            self.run_db_manager.set_root_uuid(self.root_task_uuid)

    def _task_exists(self, task_uuid: str) -> bool:
        if not task_uuid:
            return False
        if task_uuid in self.maybe_tasks_by_uuid:
            return True
        return bool(self._query_task_by_uuid(task_uuid))

    def _query_task_by_uuid(self, task_uuid: str) -> Optional[dict[str, Any]]:
        tasks = self.run_db_manager.query_tasks(task_by_uuid_exp(task_uuid))
        if not tasks:
            return None
        return tasks[0]._asdict()

    def _get_task(self, task_uuid: str) -> Optional[dict[str, Any]]:
        maybe_task = self.maybe_tasks_by_uuid.get(task_uuid)
        if maybe_task is not None:
            return maybe_task
        return self._query_task_by_uuid(task_uuid)

    def _get_incomplete_tasks(self) -> list[dict[str, Any]]:
        return [
            task
            for task in self.maybe_tasks_by_uuid.values()
            # Only incomplete tasks are kept in memory, complete tasks are None.
            if task is not None
        ]

    def _insert_new_task(self, task: dict[str, Any]) -> dict[str, Any]:
        assert 'uuid' in task, f'Cannot insert task without uuid: {task}'
        task['firex_id'] = self.firex_id
        return self.run_db_manager.insert_task(task)

    def _update_task(self, task_uuid: str, full_task: dict[str, Any], changed_data: dict[str, Any]) -> None:
        self.run_db_manager.update_task(task_uuid, changed_data)


class TaskDatabaseAggregatorThread(BrokerEventConsumerThread):
    """
        Receives Celery events and puts them on an internal
        queue to eventually store the FireX datamodel in an SQLite DB.
    """

    def __init__(
        self,
        celery_app,
        run_metadata: FireXRunMetadata,
        max_retry_attempts: int = None,
        receiver_ready_file: Optional[str] = None
    ):
        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)

        self.event_writer = KeeperThreadedEventWriter(run_metadata, self._stop_event_receiver)
        self._event_count = 0

    def _is_root_complete(self):
        return self.event_writer.is_root_complete()

    def _stop_event_receiver(self, reason):
        logger.debug(f'Keeper stopping Celery event receiver because: {reason}.')
        if self.celery_event_receiver:
            self.celery_event_receiver.should_stop = True
        else:
            logger.warning('Cannot stop event receiver because it is not initialized.')

    def _all_tasks_complete(self):
        return self.event_writer.are_all_tasks_complete()

    def _on_celery_event(self, event):
        self.event_writer.queue_celery_event(event)

        if self._event_count % 100 == 0:
            logger.debug(f'Received Celery event number {self._event_count} with task uuid: {event.get("uuid")}')
        self._event_count += 1

    def _on_cleanup(self):
        self.event_writer.stop()


class WritingFireXRunDbManager(FireXRunDbManager, KeeperEventAggregator):

    def __init__(self, run_metadata: FireXRunMetadata):
        self.run_logs_dir = run_metadata.logs_dir
        self.firex_id = run_metadata.firex_id
        self.written_celery_event_count = 0

        db_conn = connect_db(get_db_file(self.run_logs_dir, new=True), read_only=False)
        logger.info("Created DB connection.")
        Path(get_keeper_query_ready_file_path(self.run_logs_dir)).touch()

        FireXRunDbManager.__init__(self, db_conn)
        KeeperEventAggregator.__init__(self, self, self.firex_id)

    def aggregate_events_and_update_db(self, celery_events):
        try:
            changed_uuids = self._insert_or_update_tasks(celery_events)
        except (sqlalchemy.exc.DatabaseError, SqlLiteDatabaseError)  as e:
            logger.exception(e)
        else:
            # log DB write progress, similar to Celery event receive progress logging.
            for changed_uuid in changed_uuids:
                if self.written_celery_event_count % 100 == 0:
                    logger.debug(
                        'Updated Keeper DB with Celery event number '
                        f'{self.written_celery_event_count} with task uuid: {changed_uuid}')
                self.written_celery_event_count += 1

    @retry(RETRYING_DB_EXCEPTIONS)
    def insert_run_metadata(self, run_metadata: FireXRunMetadata) -> None:
        # Root UUID is not available during initialization. Populated by first task event from celery.
        run_metadata_to_insert = run_metadata._asdict()
        if RunMetadataColumn.FIREX_REQUESTER.value in run_metadata_to_insert:
            # Until we add a FIREX_REQUESTER Column in db_model.firex_run_metadata, a
            # we can't insert it. Adding the column now will not be backward compatible.
            del run_metadata_to_insert[RunMetadataColumn.FIREX_REQUESTER.value]
        self.db_conn.execute(firex_run_metadata.insert().values(**run_metadata_to_insert))

    @retry(RETRYING_DB_EXCEPTIONS)
    def _insert_or_update_tasks(self, celery_events) -> list[str]:
        with self.db_conn.begin():
            # Note querying for existing tasks during event aggregation must occur
            # within same DB transaction as insert/update, otherwise integrety errors
            # can occur.
            new_data_by_task_uuid = self.aggregate_events(celery_events)
        # in memory tracking must only occur after DB transaction success.
        self.update_in_memory_tasks(new_data_by_task_uuid)
        return list(new_data_by_task_uuid.keys()) # updated_task UUIDs.

    def insert_task(self, task) -> dict[str, Any]:
        modelled_task = get_task_data(task)
        self.db_conn.execute(firex_tasks.insert().values(**modelled_task))
        return modelled_task

    def update_task(self, uuid, changed_data) -> None:
        modelled_changed_data = get_task_data(changed_data)
        if modelled_changed_data:
            self.db_conn.execute(
                firex_tasks.update().where(firex_tasks.c.uuid == uuid).values(**modelled_changed_data)
            )

    def set_root_uuid(self, root_uuid) -> None:
        self.db_conn.execute(firex_run_metadata.update().values(root_uuid=root_uuid))

    @retry(RETRYING_DB_EXCEPTIONS)
    def _set_keeper_complete(self):
        self.db_conn.execute(firex_run_metadata.update().values(keeper_complete=True))

    def complete_writing(self):
        # set all incomplete tasks to a terminal state since we'll never
        # get any more celery events.
        self.aggregate_events_and_update_db(self.generate_incomplete_events())

        try:
            self._set_keeper_complete()
        except (sqlalchemy.exc.DatabaseError, SqlLiteDatabaseError) as e:
            logger.exception(e)

        self.close() # close DB connection.

        # TODO: confirm this won't affect cleanup operations.
        # _remove_write_permissions(get_db_file(logs_dir, new=False))
        Path(get_keeper_complete_file_path(self.run_logs_dir)).touch()