import logging
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import json
import copy

import jsonpath_ng
import socketio
from gevent import spawn, sleep
from gevent.queue import JoinableQueue

from firex_flame.flame_helper import get_dict_json_md5
from firex_flame.flame_task_graph import FlameTaskGraph, FlameModelDumper, NoWritngModelDumper


logger = logging.getLogger(__name__)


@dataclass
class _LoadedQueryConfig:

    query_config: dict[str, Any]
    model_file_name: Optional[str]
    md5_hash: str
    latest_full_query_result: dict[str, dict[str, Any]] = None
    listening_client_sids: set[str] = field(default_factory=set)

    @staticmethod
    def create_query_config(query_config, model_file_name=None):
        return _LoadedQueryConfig(
            model_file_name=model_file_name,
            # This caches jsonpath parsing, which is expensive.
            query_config=_convert_json_paths_in_query(query_config),
            md5_hash=get_dict_json_md5(query_config),
        )

    def update_latest_and_clients(
        self,
        sio_server: Optional['socketio.Server'],
        changed_uuids: list[str],
        task_graph: FlameTaskGraph,
    ) -> bool:
        latest = self.query_full_tasks(task_graph)
        updated_partial_query_result = task_graph.query_partial_tasks(
            changed_uuids,
            self.query_config,
        )
        latest.update(updated_partial_query_result)

        if (
            sio_server
            and self.listening_client_sids
            and updated_partial_query_result
        ):
            # send new data to clients listening on this query.
            sio_server.emit(
                'tasks-query-update',
                data=updated_partial_query_result,
                room=self.md5_hash)

        return bool(updated_partial_query_result)

    def add_client(self, sio_server, sid):
        if sid not in self.listening_client_sids:
            self.listening_client_sids.add(sid)
            sio_server.enter_room(sid, room=self.md5_hash)

    def query_full_tasks(self, task_graph: FlameTaskGraph, force=False):
        if self.latest_full_query_result is None or force:
            # recalc and update cached.
            self.latest_full_query_result = task_graph.query_full_tasks(self.query_config)
        return self.latest_full_query_result


@dataclass
class _QueryConfigRegistry:

    loaded_query_configs: list[_LoadedQueryConfig] = field(default_factory=list)

    def _find_config_by_hash(self, query_md5_hash):
        for config in self.loaded_query_configs:
            if config.md5_hash == query_md5_hash:
                return config
        return None

    def _find_config_by_name(self, model_file_name):
        for config in self.loaded_query_configs:
            if config.model_file_name == model_file_name:
                return config
        return None

    def _find_config(self, query_config, model_file_name):
        if model_file_name:
            config = self._find_config_by_name(model_file_name)
            if config:
                return config
        query_md5 = get_dict_json_md5(query_config)
        return self._find_config_by_hash(query_md5)

    def add_query_config(self, sio_server, query_config, model_file_name, sid):
        config = self._find_config(query_config, model_file_name)
        if not config:
            config = _LoadedQueryConfig.create_query_config(query_config, model_file_name)
            self.loaded_query_configs.append(config)

        if sio_server and sid:
            config.add_client(sio_server, sid)

    def remove_listening_client(self, sio_server, sid):
        for config in self.loaded_query_configs:
            if sid in config.listening_client_sids:
                config.listening_client_sids.remove(sid)
                sio_server.leave_room(sid, room=config.md5_hash)

    def update_latest_and_listening_clients(
        self,
        sio_server: Optional['socketio.Server'],
        changed_uuids,
        task_graph: FlameTaskGraph,
    ):
        changed_result_configs = []
        for query_config in self.loaded_query_configs:
            changed = query_config.update_latest_and_clients(
                sio_server,
                changed_uuids,
                task_graph,
            )
            if changed:
                changed_result_configs.append(query_config)
        return changed_result_configs

    def query_full_tasks(self, task_queries, task_graph: FlameTaskGraph, model_file_name, force=False):
        config = self._find_config(task_queries, model_file_name)
        if not config:
            config = _LoadedQueryConfig.create_query_config(task_queries, model_file_name)

        return config.query_full_tasks(task_graph, force=force)


class FlameAppController:

    def __init__(
        self,
        run_metadata: dict[str, Any],
        extra_task_representations=tuple(),
        dump_model=True,
        min_age_repr_dump=60,
    ):
        self.run_metadata = run_metadata
        self.min_age_repr_dump = min_age_repr_dump

        if dump_model:
            self.model_dumper = FlameModelDumper(firex_logs_dir=self.run_metadata['logs_dir'])
        else:
            self.model_dumper = NoWritngModelDumper(self.run_metadata.get('logs_dir'))

        self.graph : FlameTaskGraph = FlameTaskGraph(model_dumper=self.model_dumper)

        self.extra_task_representations = extra_task_representations
        self.query_config_registry = _QueryConfigRegistry()
        self.running_dumper_queue = RunningModelDumper(self)

        # Set after creation as a startup optimization.
        self.sio_server : Optional[socketio.Server] = None

    def update_graph_and_sio_clients(self, events: list[dict[str, Any]]) -> None:
        new_data_by_task_uuid, slim_update_data_by_uuid = self.graph.update_graph_from_celery_events(
            events,
        )
        self._update_slim_listening_sio_clients(slim_update_data_by_uuid)

        self._queue_running_dumper_writes(
            bool(slim_update_data_by_uuid),
            new_data_by_task_uuid,
        )

    def _queue_running_dumper_writes(self, slim_changes: bool, new_data_by_task_uuid):
        if slim_changes:
            self.running_dumper_queue.queue_write_slim()

        self.running_dumper_queue.queue_maybe_write_tasks(
            {
                u: self.graph.get_task_field(u, 'type')
                for u in new_data_by_task_uuid
            })

        self.running_dumper_queue.queue_maybe_write_task_reprs(
            _get_changed_uuids(new_data_by_task_uuid),
        )

    def _update_slim_listening_sio_clients(self, slim_update_data_by_uuid):
        # sio_server can be lazy initialized. Since the event receiving process starts before the
        # web modules are loaded, extremely early events can't be delivered.
        if self.sio_server:
            # Avoid sending events if there aren't fields the downstream cares about.
            if slim_update_data_by_uuid:
                self.sio_server.emit('tasks-update', slim_update_data_by_uuid)

    def dump_updated_metadata(self, update: dict[str, Any]) -> None:
        self.run_metadata.update(update)
        self.model_dumper.dump_metadata(self.run_metadata, root_complete=False, flame_complete=False)

    def _dump_task_query_config_results(
        self,
        query_config: _LoadedQueryConfig,
        force_recalc=False,
    ):
        # only dump named queries specified at launch, not ad-hoc queries from socketio clients.
        if query_config.model_file_name:
            self.model_dumper.dump_task_representation(
                query_config.model_file_name,
                tasks_representation=query_config.query_full_tasks(self.graph, force=force_recalc),
                force=force_recalc,
                min_age_change=self.min_age_repr_dump,
            )

    def dump_extra_task_representations(self, force_recalc=True) -> None:
        self.initialize_task_query_registry()

        for query_config in self.query_config_registry.loaded_query_configs:
            # forcing a full re-calc (includin loading any task that might influence the representation)
            # is is likely overkill, but it makes sure there aren't incremental accumulation errors.
            self._dump_task_query_config_results(query_config, force_recalc=force_recalc)

    def dump_full_task(self, uuid, new_event_types):
        self.graph.dump_full_task(uuid, new_event_types)

    def dump_slim_tasks(self) -> None:
        self.model_dumper.dump_slim_tasks(
            self.graph.get_slim_tasks_by_uuid()
        )

    def update_and_dump_task_representations(self, changed_task_uuids):
        changed_query_result_config = self.query_config_registry.update_latest_and_listening_clients(
            self.sio_server,
            changed_task_uuids,
            self.graph,
        )

        for query_config in changed_query_result_config:
            self._dump_task_query_config_results(query_config)

    def add_client_task_query_config(self, sid, query_config, model_file_name):
        self.query_config_registry.add_query_config(
            self.sio_server,
            query_config,
            model_file_name,
            sid)

    def remove_client_task_query(self, sid):
        self.query_config_registry.remove_listening_client(self.sio_server, sid)

    def initialize_task_query_registry(self):
        if not self.query_config_registry.loaded_query_configs:
            # parsing jsonpaths can be surprisingly slow, so do this lazily instead
            # of at construction time.
            for repr_file in self.extra_task_representations:
                task_repr = load_tasks_representation(repr_file)
                self.add_client_task_query_config(
                    sid=None,
                    query_config=task_repr['task_queries'],
                    model_file_name=task_repr['model_file_name'],
                )

    def set_sio_server(self, sio_server: 'socketio.Server'):
        self.sio_server = sio_server

    def query_full_tasks(self, task_queries, model_file_name, force=False):
        return self.query_config_registry.query_full_tasks(
            task_queries,
            self.graph,
            model_file_name,
            force=force,
        )

    def is_root_complete(self):
        return self.graph.is_root_complete()

    def is_all_tasks_complete(self):
        return self.graph.all_tasks_complete()

    def finalize_all_tasks(self):
        " Mark any incomplete tasks as fake-terminal incomplete state, update clients and dump all task data models."

        _, slim_update_data_by_uuid = self.graph.set_and_dump_any_incomplete_tasks()
        self._update_slim_listening_sio_clients(slim_update_data_by_uuid)
        self.running_dumper_queue.wait_stop()

        self.dump_extra_task_representations()
        self.model_dumper.dump_complete_data_model(
            self.graph,
            run_metadata=self.run_metadata,
            #running model dumper will dump task JSONS, don't do it twice.
            dump_task_jsons=False,
        )


def _get_changed_uuids(new_data_by_task_uuid):
    changed_uuids = list(new_data_by_task_uuid.keys())
    for new_task_data in new_data_by_task_uuid.values():
        if new_task_data.get('parent_id'):
            changed_uuids.append(new_task_data['parent_id'])
        if new_task_data.get('additional_children'):
            changed_uuids.extend(new_task_data['additional_children'])
    return changed_uuids


EXTRA_TASK_REPR_DUMP_DELAY_STEP = 5

class QueueItemType(Enum):
    SLIM_DUMP_TYPE = 'SLIM'
    TASK_DUMP_TYPE = 'TASK'
    STOP_DUMP_TYPE = 'STOP'
    EXTRA_REPR_DUMP_TYPE = 'EXTRA_REPR_DUMP_TYPE'


@dataclass
class _QueueItem:
    item_type : QueueItemType
    task_uuid: Optional[str] = None
    celery_event_type: Optional[str] = None


class RunningModelDumper:
    """
    Maintains a queue and a greenlet for async writing the flame data model.
    Having a dedicated greenlet prevents writing the model to disk
    from blocking the main celery event receiver.
    """

    def __init__(self, flame_controller: FlameAppController):
        self.flame_controller : FlameAppController = flame_controller

        # This is a JoinableQueue just to make testing easier. Clients will wait on the greenlet that processes
        # queue items, not the queue itself.
        self._queue : JoinableQueue[_QueueItem] = JoinableQueue()

        self._consume_queue_greenlet = spawn(self._consume_from_queue)

    def _deduplicate_and_maybe_write_full_tasks(self, task_dump_work_items: list[_QueueItem]) -> None:
        task_uuids_to_event_typess_completed : dict[str, set[str]] = {}
        for task_work_item in task_dump_work_items:
            uuid = task_work_item.task_uuid
            event_type = task_work_item.celery_event_type
            assert uuid is not None, "Must have task UUID for TASK_DUMP_TYPE"
            assert event_type is not None, "Must have event type for TASK_DUMP_TYPE"

            if uuid not in task_uuids_to_event_typess_completed:
                task_uuids_to_event_typess_completed[uuid] = set()
            task_uuids_to_event_typess_completed[uuid].add(event_type)

        for uuid, event_types in task_uuids_to_event_typess_completed.items():
            self.flame_controller.dump_full_task(uuid, event_types)

    def _deduplicate_and_update_task_query_results(self, task_dump_work_items: list[_QueueItem]) -> None:
        changed_uuids = set()
        for task_work_item in task_dump_work_items:
            uuid = task_work_item.task_uuid
            assert uuid is not None, "Must have task UUID for EXTRA_REPR_DUMP_TYPE"
            changed_uuids.add(uuid)

        self.flame_controller.update_and_dump_task_representations(changed_uuids)

    def _get_all_from_queue(self) -> list[_QueueItem]:
        item = self._queue.get()

        # drain queue and process all work items at once, de-duplicating work.
        return [item] + [self._queue.get() for _ in range(len(self._queue))]

    def _consume_from_queue(self) -> None:
        self.flame_controller.initialize_task_query_registry()
        consuming = True
        while consuming:
            # drain queue and process all work items at once, de-duplicating work.
            work_items : list[_QueueItem] = self._get_all_from_queue()
            work_item_types : set[QueueItemType] = {t.item_type for t in work_items}

            try:
                if QueueItemType.SLIM_DUMP_TYPE in work_item_types:
                    self.flame_controller.dump_slim_tasks()

                if QueueItemType.TASK_DUMP_TYPE in work_item_types:
                    self._deduplicate_and_maybe_write_full_tasks(
                        [t for t in work_items if t.item_type == QueueItemType.TASK_DUMP_TYPE],
                    )

                if QueueItemType.EXTRA_REPR_DUMP_TYPE in work_item_types:
                    self._deduplicate_and_update_task_query_results(
                        [t for t in work_items if t.item_type == QueueItemType.EXTRA_REPR_DUMP_TYPE],
                    )

            except Exception as e:
                # TODO: narrow exception handling so that an error in handling of one dump_type doesn't fail others.
                logger.error("Failure while processing task-dumping work queue entry.")
                logger.exception(e)
            finally:
                for _ in range(len(work_items)):
                    self._queue.task_done()

                # Must be last, want to process all other work items before we stop processing all future
                # work items.
                if QueueItemType.STOP_DUMP_TYPE in work_item_types:
                    logger.debug("Stopping in progress model dumper.")
                    consuming = False
                else:
                    # Let other greenlets run, possibly let work accumulate in the queue to allow work de-duplication
                    sleep(0.2)

    def queue_write_slim(self) -> None:
        self._queue.put(_QueueItem(QueueItemType.SLIM_DUMP_TYPE))

    def queue_maybe_write_tasks(self, task_uuids_to_event_types):
        for task_uuid, event_type in task_uuids_to_event_types.items():
            self._queue.put(_QueueItem(QueueItemType.TASK_DUMP_TYPE, task_uuid, event_type))

    def queue_maybe_write_task_reprs(self, task_uuids):
        for task_uuid in task_uuids:
            self._queue.put(_QueueItem(QueueItemType.EXTRA_REPR_DUMP_TYPE, task_uuid))

    def wait_stop(self) -> None:
        self.queue_write_slim()
        self._queue.put(_QueueItem(QueueItemType.STOP_DUMP_TYPE))
        self._consume_queue_greenlet.join() # Wait for queue to drain.


def load_tasks_representation(rep_file):
    with open(rep_file, encoding='utf-8') as fp:
        return json.load(fp)


def _convert_json_paths_in_query(task_queries):
    result = copy.deepcopy(task_queries)
    for query in result:
        if 'selectPaths' in query:
            query['selectPaths'] = [jsonpath_ng.parse(f'$.{p}') for p in query['selectPaths']]
    return result