import logging
from typing import Optional, Any, Union, Callable
import re
from datetime import datetime
import dataclasses
import os
import tarfile
from pathlib import Path
from enum import Enum
import json
import time
import shutil
import subprocess

from firexapp.events.model import ADDITIONAL_CHILDREN_KEY, EXTERNAL_COMMANDS_KEY, RunStates
from firexapp.events.event_aggregator import transform_task_state
from firex_flame.flame_helper import flatten, deep_merge
from firex_flame.model_dumper import get_all_tasks_dir, get_tasks_slim_file, get_run_metadata_file, \
    get_model_complete_file, atomic_write_json, get_flame_model_dir

from gevent.lock import BoundedSemaphore


logger = logging.getLogger(__name__)

LIST_PATH_ENTRY = re.compile(r'^\[(\d+)\]$')

TASK_TYPE = dict[str, Any] # fixme should probably data model
TASKS_BY_UUID_TYPE = dict[str, TASK_TYPE] # fixme should probably data model

TASK_ARGS = 'firex_bound_args'
_LATEST_TIMESTAMP_KEY = 'latest_timestamp'
_FIRST_STARTED_KEY = 'first_started'
_STARTED_INFO_TIMESTAMP_KEY = 'started_info_timestamp'

CeleryEvent = dict[str, Any]


def _times_from_event(event: dict[str, Any]) -> dict:
    times = {_LATEST_TIMESTAMP_KEY: event['local_received']}
    if event.get('type') == 'task-started-info':
        times[_STARTED_INFO_TIMESTAMP_KEY] = event['local_received']
    return times


# config field options:
#   copy_celery - True if this field should be copied from the celery event to the task data model. If the field already
#                   has a value on the data model, more recent celery field values will overwrite existing values by
#                   default. If overwriting should be avoided, see 'aggregate_merge' and 'callback_update'
#                   options described below.
#
#   slim_field - True if this field should be included in the 'slim' (minimal) data model representation sent to the UI.
#                   Be very careful adding fields, since this field will be sent for each node and can therefore greatly
#                   increase the amount of data sent to the UI on main graph load.
#
#   transform_celery - A function to be executed on the entire event when the corresponding key is present in a celery
#                       event. The function returns a dict that dict.update the existing data from the event, possibly
#                       overwriting data copied from the celery event by copy_celery=True. Can be used to change
#                       the field name on the data model from the field name from celery.
#
#   aggregate_merge - True if model updates should deep merge collection data types (lists, dicts, sets) instead of
#                       overwriting.
#
#
#
FIELD_CONFIG = {
    'uuid': {'copy_celery': True, 'slim_field': True},
    'hostname': {'copy_celery': True, 'slim_field': True},
    'parent_id': {'copy_celery': True, 'slim_field': True},
    'type': {
        'copy_celery': True,
        'transform_celery': transform_task_state,
    },
    'retries': {'copy_celery': True, 'slim_field': True},
    TASK_ARGS: {'copy_celery': True},
    'actual_runtime': {'copy_celery': True, 'slim_field': True},
    'support_location': {'copy_celery': True},
    'utcoffset': {'copy_celery': True},
    'code_url': {'copy_celery': True},
    # TODO: start using code_filepath instead of code_url.
    'code_filepath': {'copy_celery': True},
    'firex_default_bound_args': {'copy_celery': True},
    'from_plugin': {'copy_celery': True, 'slim_field': True},
    'chain_depth': {'copy_celery': True, 'slim_field': True},
    'firex_result': {'copy_celery': True},
    'traceback': {'copy_celery': True},
    'exception': {'copy_celery': True},
    'long_name': {
        'copy_celery': True,
        'transform_celery': lambda e: {'name': e['long_name'].split('.')[-1]},
    },
    'flame_data': {
        'copy_celery': True,
        'slim_field': True,
        'aggregate_merge': True,
    },
    'state': {'slim_field': True},
    'task_num': {'slim_field': True},
    'name': {
        'slim_field': True,
        # TODO: firexapp should send long_name, since it will overwrite 'name' copied from celery. Then get rid of
        # the following config.
        'transform_celery': lambda e: {
            'name': e['name'].split('.')[-1],
            'long_name': e['name']},
    },
    'url': {
        # TODO: only for backwards compat. Can use log_filepath.
        'transform_celery': lambda e: {'logs_url': e['url']},
    },
    'log_filepath': {
        'transform_celery': lambda e: {'logs_url': e['log_filepath']},
    },
    'local_received': {
        'transform_celery': _times_from_event,
    },
    _LATEST_TIMESTAMP_KEY: {
        'slim_field': True,
    },
    'states': {
        'aggregate_merge': True,
    },
    EXTERNAL_COMMANDS_KEY: {
        'copy_celery': True,
        'aggregate_merge': True,
    },
    ADDITIONAL_CHILDREN_KEY: {
        'copy_celery': True,
        'aggregate_merge': True,
        'slim_field': True,
    },
    'exception_cause_uuid': {
        'copy_celery': True,
        'slim_field': True,
    },
    'error_context': {
        'copy_celery': True,
        'slim_field': True,
    },
    'called_as_orig': {
        'copy_celery': True,
    },
    _FIRST_STARTED_KEY: {
        'slim_field': True,
    },
    'cached_result_from': {
        'copy_celery': True,
    },
    'pid': {
        'copy_celery': True,
    },
    'was_revoked': {
        'copy_celery': True,
    },
}


def _get_keys_with_true(input_dict, key):
    return [k for k, v in input_dict.items() if v.get(key, False)]


COPY_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'copy_celery')
# These are the minimum fields required to render the graph.
SLIM_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'slim_field')

AGGREGATE_MERGE_FIELDS = _get_keys_with_true(FIELD_CONFIG, 'aggregate_merge')
FIELD_TO_CELERY_TRANSFORMS = {
    k: v['transform_celery'] for k, v in FIELD_CONFIG.items()
    if 'transform_celery' in v
}


class _TaskFieldSentile(Enum):
    UNSET = 1
    UNLOADED = 2

@dataclasses.dataclass
class _ModelledFlameTask:
    firex_bound_args: Union[dict, _TaskFieldSentile] = _TaskFieldSentile.UNSET
    firex_default_bound_args: Union[dict, _TaskFieldSentile] = _TaskFieldSentile.UNSET
    firex_result: Union[dict, _TaskFieldSentile] = _TaskFieldSentile.UNSET
    external_commands: Union[dict, _TaskFieldSentile] = _TaskFieldSentile.UNSET

    def as_dict(self):
        return {
            field_name: getattr(self, field_name)
            for field_name in self.unloadable_field_names()
            if not isinstance(getattr(self, field_name), _TaskFieldSentile)
        }

    def get_set_field_names(self):
        return [
            field_name for field_name in self.unloadable_field_names()
            if getattr(self, field_name) != _TaskFieldSentile.UNSET
        ]

    def unloadable_field_names(self):
        return [f.name for f in dataclasses.fields(self)]

    def any_unloaded(self, field_names=None) -> bool:
        if field_names is None:
            field_names = self.unloadable_field_names()
        return any(
            getattr(self, f) == _TaskFieldSentile.UNLOADED
            for f in field_names
        )

    def unload_fields(self):
        for field_name in self.unloadable_field_names():
            field_val = getattr(self, field_name)
            if not isinstance(field_val, _TaskFieldSentile):
                # allow this field's value to be garbage collected by unreferencing it.
                setattr(self, field_name, _TaskFieldSentile.UNLOADED)
                del field_val

    def update(self, update_dict):
        for k in self.unloadable_field_names():
            if k in update_dict:
                setattr(self, k, update_dict[k])


def is_task_dict_complete(task_dict) -> bool:
    return RunStates.is_complete_state(
        task_dict.get('state'),
        has_completed=task_dict.get('has_completed'),
    )


@dataclasses.dataclass
class _FlameTask:
    _modelled : _ModelledFlameTask
    always_loaded_task_data: dict[str, Any]
    model_dumper : 'FlameModelDumper'
    _lock = BoundedSemaphore()

    @staticmethod
    def create_task(task_uuid, task_num, model_dumper : 'FlameModelDumper'):
        return _FlameTask(
            _ModelledFlameTask(),
            always_loaded_task_data={
                'uuid': task_uuid,
                'state': None,
                'task_num': task_num,
                'was_revoked': False,
                # Indicates the completed event has been received,
                # DOES NOT GUARANTEE ALL TASK DATA HAS BEEN RECEIVED.
                # task-success (with results) can be received after task-completed,
                # for example.
                'has_completed': False,
            },
            model_dumper=model_dumper
        )

    def as_dict(self) -> dict[str, Any]:
        return self.always_loaded_task_data | self._modelled.as_dict()

    def _load_from_full_task_file(self):
        assert self._lock.locked()
        try:
            return self.model_dumper.load_full_task(self.get_uuid())
        except OSError as e:
            logger.warning(f"Failed to load full task for {self.get_uuid()} due to: {e}")
            return {}

    def _already_locked_get_full_task_dict(self, field_names=None):
        assert self._lock.locked()
        if self._modelled.any_unloaded(field_names):
            modeled_dict = self._load_from_full_task_file()
        else:
            modeled_dict = self._modelled.as_dict()
        return modeled_dict | self.always_loaded_task_data

    def get_full_task_dict(self):
        with self._lock:
            return self._already_locked_get_full_task_dict()

    def get_uuid(self):
        return self.always_loaded_task_data['uuid']

    def dump_full(self, new_event_types: set[str]):
        should_dump = (
            # dump when we receive args
            'task-started-info' in new_event_types

            # dump when the task is complete (or if it was already complete, regardless of event type, due to out-of-order events.)
            # FIXME: is_completed can cause double dumps for successes when completed is received after success.
            or self.is_complete()
        )
        if should_dump:
            with self._lock:
                full_task_dict = self._already_locked_get_full_task_dict()
                try:
                    self.model_dumper.dump_full_task(self.get_uuid(), full_task_dict)
                except OSError as e:
                    logger.warning(f"Failed to write {self.get_uuid()} full task JSON: {e}")
                else:
                    if self.is_complete():
                        # TODO: is it too much to always unload completed?
                        self._modelled.unload_fields()
                        task_num = full_task_dict['task_num']
                        if task_num % 100 == 0:
                            logger.debug(f'Unloaded big fields for task num {task_num}, uuid {self.get_uuid()}')

    def get_task_state(self) -> Optional[RunStates]:
        try:
            return RunStates.create(self.always_loaded_task_data['state'])
        except TypeError:
            return None

    def is_complete(self) -> bool:
        return is_task_dict_complete(self.always_loaded_task_data)

    def is_field_set(self, field_name) -> bool:
        return self.get_field(field_name, default=_TaskFieldSentile.UNSET) != _TaskFieldSentile.UNSET

    def get_field(self, field_name, default=_TaskFieldSentile.UNSET):
        return self.get_fields([field_name]).get(field_name, default)

    def get_fields(self, field_names):
        if set(field_names).isdisjoint(self._modelled.unloadable_field_names()):
            # no need to load unloaded data.
            task_dict = self.always_loaded_task_data
        else:
            with self._lock: # unloaded field requested
                task_dict = self._already_locked_get_full_task_dict(field_names)

        return {
            k: task_dict[k]
            for k in field_names
            if k in task_dict
        }

    def update(self, update_dict: dict[str, Any]):
        self._modelled.update(
            {
                k: v for k, v in update_dict.items()
                if k in self._modelled.unloadable_field_names()
            }
        )
        self.always_loaded_task_data.update(
            {
                k: v for k, v in update_dict.items()
                if k not in self._modelled.unloadable_field_names()
            }
        )

    def get_field_names(self):
        return list(self.always_loaded_task_data.keys()) + self._modelled.get_set_field_names()

    def find_task_changes(self, new_task_data: dict[str, Any]) -> dict[str, Any]:
        """
            Note new_task_data is data from the Celery Event as specified by FIELD_CONFIG

            This method does not apply changes to self, it just calculates changes to be made.
        """

        changed_data = {}
        for new_field_name, new_field_value in new_task_data.items():
            if new_field_name == _STARTED_INFO_TIMESTAMP_KEY:
                # this timestamp should be kept once set; it's the most accurate.
                if not self.get_field('retries', 0):
                    changed_data[_FIRST_STARTED_KEY] = new_field_value
            elif new_field_name == _LATEST_TIMESTAMP_KEY:
                changed_data[_LATEST_TIMESTAMP_KEY] = new_field_value
                # only accept latest as first_started timestamp if we have no started timestamp
                if not self.is_field_set(_FIRST_STARTED_KEY):
                    changed_data[_FIRST_STARTED_KEY] = new_field_value
            elif new_field_name in AGGREGATE_MERGE_FIELDS:
                existing_field_value = self.get_field(
                    # default to empty instance of same type
                    new_field_name, default=type(new_field_value)())
                new_merged_value = deep_merge(existing_field_value, new_field_value)
                if not self.is_field_set(new_field_name) or existing_field_value != new_merged_value:
                    changed_data[new_field_name] = new_merged_value
            else:
                # Some fields overwrite whatever is present. Be permissive, since not all fields captured are from celery,
                # so not all have entries in the field config.
                if self.get_field(new_field_name) != new_field_value:
                    changed_data[new_field_name] = new_field_value

        return changed_data


class FlameTaskGraph:

    def __init__(
        self,
        tasks_by_uuid: Optional[TASKS_BY_UUID_TYPE] = None,
        model_dumper: Optional['FlameModelDumper'] = None,
    ):
        self.root_uuid : Optional[str] = None

        self._tasks_by_uuid : dict[str, _FlameTask] = {}
        self.model_dumper = model_dumper
        self._event_aggregator = FlameEventAggregator(
            self._tasks_by_uuid, # Must share instance.
            self.model_dumper)

        # cache relationships for faster querying for ancestors and descedants.
        self._parent_to_children_uuids: dict[str, set[str]] = {}
        self._child_to_parents_uuids: dict[str, set[str]] = {}

        # add input task to graph's instance.
        self.update_graph_from_celery_events(
            (tasks_by_uuid or {}).values()
        )

    def get_root_task(self) -> Optional[TASK_TYPE]:
        if not self.root_uuid:
            return None
        return self.get_full_task_dict(self.root_uuid)

    def _walk_task_graph(self, task_uuid, uuids_by_uuid) -> list[_FlameTask]:
        checked_uuids = set()
        to_check_uuids = [task_uuid]
        result_tasks_by_uuid = {}
        while to_check_uuids:
            uuid = to_check_uuids.pop()
            checked_uuids.add(uuid)
            for related_uuid in uuids_by_uuid.get(uuid, []):
                if related_uuid in self._tasks_by_uuid:
                    result_tasks_by_uuid[related_uuid] = self._tasks_by_uuid[related_uuid]
                if related_uuid not in checked_uuids:
                    to_check_uuids.append(related_uuid)
        return list(result_tasks_by_uuid.values())

    def get_descendants_of_uuid(self, task_uuid) -> list[_FlameTask]:
        return self._walk_task_graph(
            task_uuid,
            self._parent_to_children_uuids,
        )

    def get_ancestors_of_uuid(self, task_uuid) -> list[_FlameTask]:
        return self._walk_task_graph(
            task_uuid,
            self._child_to_parents_uuids,
        )

    def _add_parent_and_child(self, parent_uuid, child_uuid):
        if parent_uuid not in self._parent_to_children_uuids:
            self._parent_to_children_uuids[parent_uuid] = set()
        self._parent_to_children_uuids[parent_uuid].add(child_uuid)

        if child_uuid not in self._child_to_parents_uuids:
            self._child_to_parents_uuids[child_uuid] = set()
        self._child_to_parents_uuids[child_uuid].add(parent_uuid)

    def _maybe_set_root_uuid(self, events):
        for event in events:
            if self.root_uuid is None:
                if (
                    event.get('parent_id', '__no_match') is None
                    and event.get('uuid')
                ):
                    self.root_uuid = event['uuid']
                elif event.get('root_id') is not None:
                    # we can still know the root if we miss the first event (the root's event with parent_id)
                    # since other events reference the root UUID via root_id.
                    self.root_uuid = event['root_id']

    def _update_graph_from_task_data(self, task_uuid, event):
        if task_uuid:
            parent_id = event.get('parent_id')
            if parent_id:
                self._add_parent_and_child(parent_id, task_uuid)

            additional_children = event.get(ADDITIONAL_CHILDREN_KEY)
            if additional_children:
                for child_uuid in additional_children:
                    self._add_parent_and_child(task_uuid, child_uuid)

    def update_graph_from_celery_events(self, events):
        self._maybe_set_root_uuid(events)

        # find only data changes from events.
        new_data_by_task_uuid = self._event_aggregator.aggregate_events(events)
        for task_uuid, new_task_data in new_data_by_task_uuid.items():
            self._update_graph_from_task_data(task_uuid, new_task_data)

        slim_update_data_by_uuid = {
            uuid: task
            for uuid, task in _slim_tasks_by_uuid(new_data_by_task_uuid).items()
            if task
        }
        return new_data_by_task_uuid, slim_update_data_by_uuid

    def query_partial_tasks(self, query_task_uuids, task_queries):
        # When querying a partial set of tasks, count descendants as matches to be included in the result.
        return _query_flame_tasks(
            self,
            query_task_uuids,
            task_queries,
            match_descendant_criteria=True)

    def query_full_tasks(self, task_queries):
        # When querying a full set of tasks, descendants will be included when their ancestors are matched.
        return _query_flame_tasks(
            self,
            self.get_all_task_uuids(),
            task_queries,
            match_descendant_criteria=False)

    def is_root_started(self) -> bool:
        task = self._tasks_by_uuid.get(self.root_uuid)
        if task is None:
            return False
        return task.get_task_state() not in [None, RunStates.RECEIVED]

    def is_root_complete(self) -> bool:
        task = self._tasks_by_uuid.get(self.root_uuid)
        if task is None:
            return False
        return task.is_complete()

    def all_tasks_complete(self):
        if not self._tasks_by_uuid:
            # do not want "no tasks received yet"
            # to look like "all tasks complete"
            return False

        # In case checking all tasks is expensive, check root first. Remaining
        # tasks only need to be checked once root is complete since everything
        # can't be complete if the root is not complete.
        if not self.is_root_complete():
            return False

        return all(
            t.is_complete() for t in self._tasks_by_uuid.values()
        )

    def get_full_task_dict(self, uuid) -> Optional[dict[str, dict[str, Any]]]:
        maybe_full_task = self._tasks_by_uuid.get(uuid)
        if maybe_full_task is None:
            return None
        return maybe_full_task.get_full_task_dict()

    def get_slim_task_dict(self, uuid) -> Optional[dict[str, dict[str, Any]]]:
        task = self._tasks_by_uuid.get(uuid)
        if task is None:
            return None
        return task.get_fields(SLIM_FIELDS)

    def get_task_field(self, uuid, field_name, default=None):
        task = self._tasks_by_uuid.get(uuid)
        if task is None:
            return default
        return task.get_field(field_name, default)

    def get_all_task_uuids(self):
        return set(self._tasks_by_uuid.keys())

    def get_slim_tasks_by_uuid(self):
        return self.get_all_tasks_fields(SLIM_FIELDS)

    def get_all_tasks_fields(self, fields):
        return {
            uuid: task.get_fields(fields)
            for uuid, task in self._tasks_by_uuid.items()
        }

    def get_full_tasks_by_uuid(self) -> TASKS_BY_UUID_TYPE:
        return {u: t.get_full_task_dict() for u, t in self._tasks_by_uuid.items()}

    def set_and_dump_any_incomplete_tasks(self):
        # Create new events that change the run state of incomplete events.
        incomplete_task_events = self._event_aggregator.generate_incomplete_events()
        if incomplete_task_events:
            logger.warning(f"Forcing runstates of {len(incomplete_task_events)} incomplete tasks to be terminal.")
            new_data_by_task_uuid, slim_update_data_by_uuid = self.update_graph_from_celery_events(incomplete_task_events)
        else:
            logger.debug("All tasks already terminal following terminal root.")
            new_data_by_task_uuid = slim_update_data_by_uuid = {}

        for uuid in new_data_by_task_uuid:
            self._tasks_by_uuid[uuid].dump_full(set())

        return new_data_by_task_uuid, slim_update_data_by_uuid

    def dump_full_task(self, uuid: str, new_event_types: set[str]):
        if uuid not in self._tasks_by_uuid:
            logger.warning(f'Ignoring request to dump non-existant tast {uuid}')
        else:
            self._tasks_by_uuid[uuid].dump_full(new_event_types)

    def _get_task(self, uuid) -> Optional[_FlameTask]:
        # Everything outside this module should do get_full_task_dict instead of this.
        return self._tasks_by_uuid.get(uuid)

    def was_revoked(self, uuid) -> bool:
        task = self._get_task(uuid)
        if task is None:
            return False
        return task.get_field('was_revoked', False)

def _slim_tasks_by_uuid(tasks_by_uuid):
    return {uuid: {k: v for k, v in task_data.items()
                   if k in SLIM_FIELDS}
            for uuid, task_data in tasks_by_uuid.items()}

def _validate_task_queries(task_representation):
    if not isinstance(task_representation, list):
        return False

    missing_criterias = [r for r in task_representation
                         if 'matchCriteria' not in r
                            or not isinstance(r['matchCriteria'], dict)]
    if missing_criterias:
        return False

    # TODO: validate matchCriteria themselves

    return True


def _normalize_criteria_key(k):
    return k[1:] if k.startswith('?') else k


def _matches_equal_criteria(task: _FlameTask, eq_criteria: dict[str, Any]):
    # TODO: if more adjusting qualifiers are added, this needs to be reworked.
    required_keys = {k for k in eq_criteria.keys() if not k.startswith('?')}
    optional_keys = {_normalize_criteria_key(k) for k in eq_criteria.keys() if k.startswith('?')}

    task_fields = task.get_field_names()
    if not required_keys.issubset(task_fields):
        return False # task is missing required keys, can't match.

    queried_field_names = required_keys.union(optional_keys)
    normalized_criteria = {_normalize_criteria_key(k): v for k, v in eq_criteria.items()}
    for task_field in task_fields: # ordered to avoid loading unloadable fields
        if (
            task_field in queried_field_names
            and task.get_field(task_field) != normalized_criteria[task_field]
        ):
            return False
    return True

def _matches_has_key_criteria(task: _FlameTask, key_path):
    if isinstance(key_path, str):
        key_path = key_path.split('.')

    if key_path and isinstance(key_path, list):
        first_key = key_path[0]
        # try to avoid loading full task for in-memory keys instead
        # of getting full task dict.
        first_key_val = task.get_field(first_key)
        if first_key_val != _TaskFieldSentile.UNSET:
            remaining_keys = key_path[1:]
            if not remaining_keys:
                return True
            elif isinstance(first_key_val, dict):
                tmp_dict = first_key_val
                for k in remaining_keys:
                    try:
                        tmp_dict = tmp_dict[k]
                    except Exception:
                        return False
                return True
    return False


def task_matches_criteria(task: _FlameTask, criteria: dict):
    if criteria['type'] == 'all':
        return True

    if criteria['type'] == 'always-select-fields':
        # always-select-fields doesn't cause matches (tasks to be included), but paths here are always included
        # in results.
        return False

    if criteria['type'] == 'equals':
        return _matches_equal_criteria(task, criteria['value'])

    if criteria['type'] == 'has-key':
        return _matches_has_key_criteria(task, criteria['value'])

    return False


def _add_path_to_container(container, path_list, val):
    if not path_list:
        return
    if len(path_list) == 1:
        final_key = path_list[0]
        is_list = LIST_PATH_ENTRY.match(final_key)
        if is_list:
            container.append(val)
        else:
            container[final_key] = val
    else:
        cur_key = path_list.pop(0)
        is_cur_list = LIST_PATH_ENTRY.match(cur_key)
        if is_cur_list:
            cur_key = int(is_cur_list.group(1))

def _add_path_to_container(top_container: dict[str, Any], path_list: tuple[Union[str,int]], val):
    latest_container = top_container
    for i, cur_key in enumerate(path_list):
        is_last_key = i == len(path_list) - 1
        is_latest_list = isinstance(latest_container, list)

        if is_latest_list:
            latest_cont_keys = range(len(latest_container))
        else:
            latest_cont_keys = latest_container.keys()

        if is_last_key:
            # Last key, set value instead of finding next container.
            if is_latest_list and cur_key not in latest_cont_keys:
                latest_container.append(val)
            else:
                latest_container[cur_key] = val
        else:
            # not last key, find or create next container.
            if cur_key not in latest_cont_keys:
                is_next_list = isinstance(path_list[i+1], int)
                if is_next_list:
                    next_container = []
                else:
                    next_container = {}

                if is_latest_list:
                    latest_container.append(next_container) # assume integer keys pre-sorted.
                else:
                    latest_container[cur_key] = next_container
            else:
                next_container = latest_container[cur_key]
            latest_container = next_container


def _int_index_or_key(json_key_part_str):
    m = LIST_PATH_ENTRY.match(json_key_part_str)
    if m:
        return int(m.group(1))
    return json_key_part_str


def _container_from_json_paths_to_values(json_paths_to_values: list[str]):

    container = {}
    parsed_key_paths_to_values = {
        tuple(_int_index_or_key(str_k_part) for str_k_part in k.split('.')): v
        for k, v in json_paths_to_values.items()
    }
    for path_tuple, value in sorted(parsed_key_paths_to_values.items(), key=lambda kv: kv[0]):
        _add_path_to_container(
            container,
            path_tuple,
            value)
    return container


def _jsonpath_get_paths(jsonpath_exprs, task_dict):
    matching_paths_to_values = {}
    for jsonpath_expr in jsonpath_exprs:
        matching_paths_to_values.update(
            {
                str(match.full_path): match.value
                for match in jsonpath_expr.find(task_dict)
            }
        )
    return _container_from_json_paths_to_values(matching_paths_to_values)


def _get_descendants_for_criteria(select_paths, descendant_criteria, ancestor_uuid, task_graph: FlameTaskGraph):
    ancestor_descendants = task_graph.get_descendants_of_uuid(ancestor_uuid)
    matched_descendants_by_uuid = {}
    for criteria in descendant_criteria:
        for descendant in ancestor_descendants:
            if task_matches_criteria(descendant, criteria):
                # The fields that are selected for each descendant are determined by all queries, except
                # descendant descendants are never included.
                matched_descendants_by_uuid[descendant.get_uuid()] = select_from_task(
                    select_paths,
                    [],  # Never include descendants in descendant queries to avoid infinite loop.
                    descendant,
                    task_graph)

    return matched_descendants_by_uuid


def select_from_task(
    select_paths,
    select_descendants,
    task: _FlameTask,
    task_graph: FlameTaskGraph,
):
    selected_dict = {}
    # FIXME: get_full_task_dict may load here even when the paths don't need it to!
    paths_update_dict = _jsonpath_get_paths(select_paths, task.get_full_task_dict())
    selected_dict.update(paths_update_dict)

    selected_descendants_by_uuid = _get_descendants_for_criteria(
        select_paths,
        select_descendants,
        task.get_uuid(),
        task_graph)

    if selected_descendants_by_uuid:
        selected_dict.update({'descendants': selected_descendants_by_uuid})

    return selected_dict


def get_always_select_fields(task_queries):
    return flatten([q.get('selectPaths', []) for q in task_queries
                    if q['matchCriteria']['type'] == 'always-select-fields'])


def _select_ancestors_of_task_descendant_match(
    desc_task: _FlameTask,
    all_task_queries,
    task_graph: FlameTaskGraph,
):
    # Should the current task be included in the result because it matches some descendant criteria?
    ancestor_results_by_uuid = {}

    # find query matched by desc so only possibly affected ancestors will be re-queried.
    desc_matching_queries = [
        task_query for task_query in all_task_queries
        if any(
            task_matches_criteria(desc_task, desc_query)
            for desc_query in task_query.get('selectDescendants', []))
    ]
    if desc_matching_queries:
        # The current task matches some descendant criteria. Find all ancestors that match top-level criteria
        # and return those ancestors.
        always_select_fields = get_always_select_fields(all_task_queries)
        for ancestor_task in task_graph.get_ancestors_of_uuid(desc_task.get_uuid()):
            ancestor_query_result = _query_task(
                ancestor_task,
                desc_matching_queries,
                task_graph,
                always_select_fields=always_select_fields
            )
            if ancestor_query_result is not None:
                ancestor_results_by_uuid[ancestor_task.get_uuid()] = ancestor_query_result

    return ancestor_results_by_uuid


def _query_task(
    task: _FlameTask,
    all_task_queries,
    task_graph: FlameTaskGraph,
    always_select_fields=None
):
    matching_queries = [
        query for query in all_task_queries
        if task_matches_criteria(task, query['matchCriteria'])
    ]
    if matching_queries:
        always_select_fields = always_select_fields or get_always_select_fields(all_task_queries)
        select_paths = always_select_fields + flatten([q.get('selectPaths', []) for q in matching_queries])
        all_select_descendants = flatten([q.get('selectDescendants', []) for q in matching_queries])
        return select_from_task(
            select_paths,
            all_select_descendants,
            task,
            task_graph,
        )
    return None


def _select_data_for_matches(task_uuid, task_queries, task_graph: FlameTaskGraph, match_descendant_criteria):
    result_tasks_by_uuid = {}
    task = task_graph._get_task(task_uuid)
    if task is not None:
        query_result = _query_task(task, task_queries, task_graph)
        if query_result is not None:
            result_tasks_by_uuid[task_uuid] = query_result

        if match_descendant_criteria:
            # For incremental updates, find all ancestors with descedant queries that match
            # "task" and query their results too.
            ancestors_by_uuid = _select_ancestors_of_task_descendant_match(
                task,
                task_queries,
                task_graph,
            )
            result_tasks_by_uuid.update(ancestors_by_uuid)

    return result_tasks_by_uuid


def _query_flame_tasks(task_graph: FlameTaskGraph, task_uuids_to_query, task_queries, match_descendant_criteria):
    if not _validate_task_queries(task_queries):
        return {}

    result_tasks_by_uuid = {}
    for uuid in task_uuids_to_query:
        selected_tasks_by_uuid = _select_data_for_matches(uuid, task_queries, task_graph, match_descendant_criteria)
        # Every query result is a full task query, no need for merging. Could de-dupe by senidng "already seen"
        # task uuids to avoid re-querying.
        result_tasks_by_uuid.update(selected_tasks_by_uuid)

    return result_tasks_by_uuid


# Event data extraction/transformation without current state context.
def _get_event_task_data(event: CeleryEvent) -> dict[str, Any]:
    new_task_data = {}
    for field in COPY_FIELDS:
        if field in event:
            new_task_data[field] = event[field]

    # Note if a field is both a copy field and a transform, the transform overrides if the output writes to the same
    # key.
    for field, transform in FIELD_TO_CELERY_TRANSFORMS.items():
        if field in event:
            new_task_data.update(transform(event))

    return new_task_data


def find_data_changes(
    task: _FlameTask,
    new_task_data: dict[str, Any],
) -> dict[str, Any]:

    changed_data = task.find_task_changes(new_task_data)

    # Some fields need to be accumulated across events, not overwritten from latest event.
    merged_fields_to_values = {
        field_name: deep_merge(
            task.get_field(
                field_name,
                # default to empty instance of same type
                default=type(new_value)(),
            ),
            new_value,
        )
        for field_name, new_value in new_task_data.items()
        if field_name in AGGREGATE_MERGE_FIELDS
    }
    for merged_data_key, merged_data_val in merged_fields_to_values.items():
        if task.get_field(merged_data_key) != merged_data_val:
            changed_data[merged_data_key] = merged_data_val

    return changed_data


class FlameEventAggregator:
    """ Aggregates many events in to the task data model. """

    def __init__(self, tasks_by_uuid, model_dumper: Optional['FlameModelDumper']=None):
        self.model_dumper = model_dumper
        self._tasks_by_uuid : dict[str, _FlameTask] = tasks_by_uuid
        self.new_task_num : int = len(tasks_by_uuid) + 1

    def aggregate_events(self, events):
        new_data_by_task_uuid = {}
        for e in events:
            event_new_data_by_task_uuid = self._aggregate_event(e)
            for uuid, new_data in event_new_data_by_task_uuid.items():
                if uuid not in new_data_by_task_uuid:
                    new_data_by_task_uuid[uuid] = {}
                new_data_by_task_uuid[uuid].update(new_data)
        return new_data_by_task_uuid

    def generate_incomplete_events(self):
        """
        Unfortunately, if a run terminates ungracefully, incomplete tasks will never arrive at a
        terminal runstate. The 'task-incomplete' runstate is a fake (non-Celery) terminal runstate
        that is generated here so that the UI can show a non-incomplete runstate.
        :return:
        """
        now = datetime.now().timestamp()
        return [
            {
                'uuid': task.get_uuid(),
                'type': RunStates.get_forced_complete_celery_event_type(
                    task.get_field('state'),
                    task.get_field('has_completed', False),
                ),
                'actual_runtime': now - task.get_field(_FIRST_STARTED_KEY, now)
            }
            for task in self._tasks_by_uuid.values()
            if not task.is_complete()
        ]

    def _get_or_create_task(self, task_uuid) -> tuple[_FlameTask, bool]:
        if task_uuid not in self._tasks_by_uuid:
            task = _FlameTask.create_task(
                task_uuid,
                self.new_task_num,
                self.model_dumper)
            self.new_task_num += 1
            self._tasks_by_uuid[task_uuid] = task
            is_new = True
        else:
            task = self._tasks_by_uuid[task_uuid]
            is_new = False
        return task, is_new

    def _aggregate_event(self, event: dict[str, Any]):
        task_uuid: Optional[str] = event.get('uuid')
        if (
            # The uuid can be null, it's unclear what this means but the event
            # can't be associated with a task so dropping is OK.
            not task_uuid
            # Revoked events can be sent before any other, and we'll never get any data (name, etc) for that task.
            # Therefore ignore events that are for a new UUID that have revoked type.
            or (
                task_uuid not in self._tasks_by_uuid
                and event.get('type') == RunStates.REVOKED.to_celery_event_type()
            )
        ):
            return {}

        task, is_new_task = self._get_or_create_task(task_uuid)
        new_task_data = _get_event_task_data(event)
        changed_data = task.find_task_changes(new_task_data)
        task.update(changed_data)

        return {
            # If we just created the task, we need to send the auto-initialized fields, as well as data from the event.
            # If this isn't a new event, we only need to send what has changed.
            task_uuid: task.as_dict() if is_new_task else changed_data
        }


class FlameModelDumper:

    def __init__(self, firex_logs_dir=None, root_model_dir: Optional[str]=None):
        assert bool(firex_logs_dir) ^ bool(root_model_dir), \
            "Dumper needs exclusively either logs dir or root model dir."
        if firex_logs_dir:
            self.root_model_dir = get_flame_model_dir(firex_logs_dir)
        else:
            assert root_model_dir
            self.root_model_dir = root_model_dir
        os.makedirs(self.root_model_dir, exist_ok=True)

        self.full_tasks_dir = get_all_tasks_dir(root_model_dir=self.root_model_dir)
        os.makedirs(self.full_tasks_dir, exist_ok=True)
        self.slim_tasks_file = get_tasks_slim_file(root_model_dir=self.root_model_dir)

    def dump_metadata(self, run_metadata, root_complete, flame_complete):
        metadata_model_file = get_run_metadata_file(root_model_dir=self.root_model_dir)
        complete = {'run_complete': root_complete, 'flame_recv_complete': flame_complete}
        atomic_write_json(metadata_model_file, run_metadata | complete)
        return metadata_model_file

    def _get_full_task_file_path(self, uuid):
        return os.path.join(self.full_tasks_dir, f'{uuid}.json')

    def dump_full_task(self, uuid, task):
        atomic_write_json(self._get_full_task_file_path(uuid), task)

    def load_full_task(self, uuid):
        with open(self._get_full_task_file_path(uuid), encoding='utf-8') as fp:
            return json.load(fp)

    def dump_slim_tasks(self, slim_tasks_by_uuid: dict[str, dict[str, Any]]):
        atomic_write_json(
            self.slim_tasks_file,
            slim_tasks_by_uuid)

    def dump_complete_data_model(
        self,
        task_graph: FlameTaskGraph,
        run_metadata=None,
        dump_task_jsons=True,
    ):
        logger.info("Starting to dump complete Flame model.")

        if dump_task_jsons:
            # Write JSON file with minimum amount of info to render graph.
            self.dump_slim_tasks(task_graph.get_slim_tasks_by_uuid())

            # Write one JSON file per task.
            for uuid, task in task_graph.get_full_tasks_by_uuid().items():
                self.dump_full_task(uuid, task)
            logger.info("Completed dumping Flame tasks.")

        paths_to_compress = [self.slim_tasks_file, self.full_tasks_dir]
        if run_metadata:
            # Write metadata file.
            # Note that since a flame can terminate (e.g. via timeout) before a run, there is no guarantee
            # that the run_metadata model file will ever have root_complete: true.
            root_uuid = task_graph.root_uuid
            root_complete = task_graph.is_root_complete()
            run_metadata_with_root = {**run_metadata, 'root_uuid': root_uuid}
            metadata_model_file = self.dump_metadata(run_metadata_with_root, root_complete, flame_complete=True)
            paths_to_compress.append(metadata_model_file)

        # Write a tar.gz file containing all the files dumped above.
        _dump_full_task_state_archive(self.root_model_dir, paths_to_compress)

        Path(get_model_complete_file(root_model_dir=self.root_model_dir)).touch()
        logger.info("Finished dumping complete Flame model.")

    def dump_task_representation(
        self,
        model_file_name: str,
        tasks_representation: dict[str, Any],
        force=False,
        min_age_change=0,
    ):
        out_file = os.path.join(self.root_model_dir, model_file_name)
        try:
            should_write = (
                force
                or not os.path.exists(out_file)
                or time.time() - os.path.getmtime(out_file) > min_age_change
            )
            if should_write:
                logger.info(f"Starting to dump task representation of: {model_file_name}.")
                atomic_write_json(out_file, tasks_representation)
                logger.info(f"Finished dumping {len(tasks_representation)} task representation of {model_file_name} to {out_file}.")
        except Exception as ex:
            # Don't interfere with shutdown even if extra representation dumping fails.
            logger.error(f"Failed to dump representation of {model_file_name}.")
            logger.exception(ex)


class NoWritngModelDumper(FlameModelDumper):

    def __init__(self, firex_logs_dir):
        super().__init__(firex_logs_dir)

    def dump_full_task(self, *args, **kwargs):
        pass

    def dump_slim_tasks(self, *args, **kwargs):
        pass

    def dump_complete_data_model(self, *args, **kwargs):
        pass

    def dump_task_representation(self, *args, **kwargs):
        pass


def _dump_full_task_state_archive(
    root_model_dir,
    paths_to_compress
):
    logger.info("Starting to create full task state archive.")
    full_state_gz_basename = 'full-run-state.tar.gz'
    tar_bin = shutil.which('tar')
    if tar_bin:
        rel_paths_to_compress = [os.path.relpath(p, root_model_dir) for p in paths_to_compress]
        try:
            subprocess.run(
                [tar_bin, 'czf', full_state_gz_basename] + rel_paths_to_compress,
                cwd=root_model_dir,
                timeout=5*60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            pass
    else:
        with tarfile.open(os.path.join(root_model_dir, full_state_gz_basename), "w:gz") as tar:
            for path in paths_to_compress:
                tar.add(path, arcname=os.path.basename(path))
    logger.info("Completed creating full task state archive.")