"""
Aggregates events in to the task data model.
"""
from collections import namedtuple
from datetime import datetime
import logging

from firexapp.events.model import ALL_RUNSTATES, INCOMPLETE_RUNSTATES, COMPLETE_RUNSTATES, TaskColumn

logger = logging.getLogger(__name__)


EventAggregatorConfig = namedtuple('EventAggregatorConfig',
                                   ['copy_fields', 'merge_fields', 'keep_initial_fields', 'field_to_celery_transforms'])


#
# config field options:
#   copy_celery - True if this field should be copied from the celery event to the task data model. If the field already
#                   has a value on the data model, more recent celery field values will overwrite existing values by
#                   default. If overwriting should be avoided, see 'aggregate_merge' and 'aggregate_keep_initial'
#                   options described below.
#
#   transform_celery - A function to be executed on the entire event when the corresponding key is present in a celery
#                       event. The function returns a dict that dict.update the existing data from the event, possibly
#                       overwriting data copied from the celery event by copy_celery=True. Can be used to change
#                       the field name on the data model from the field name from celery.
#
#   aggregate_merge - True if model updates should deep merge collection data types (lists, dicts, sets) instead of
#                       overwriting.
#
#   aggregate_keep_initial - True if data model field updates should be ignored after an initial value has been set.
#                               This is one way of preventing overwriting, see also 'aggregate_merge'.
#
#
FIELD_CONFIG = {
    TaskColumn.UUID.value: {'copy_celery': True},
    TaskColumn.HOSTNAME.value: {'copy_celery': True},
    TaskColumn.PARENT_ID.value: {'copy_celery': True},
    'type': {
        'copy_celery': True,
        'transform_celery': lambda e: {
            'state': e['type'],
            'states': [{TaskColumn.STATE.value: e['type'],
                        'timestamp': e.get('timestamp', None)}],
        } if e['type'] in ALL_RUNSTATES else {},
    },
    TaskColumn.RETRIES.value: {'copy_celery': True},
    TaskColumn.BOUND_ARGS.value: {'copy_celery': True},
    TaskColumn.ACTUAL_RUNTIME.value: {'copy_celery': True},
    TaskColumn.UTCOFFSET.value: {'copy_celery': True},
    TaskColumn.DEFAULT_BOUND_ARGS.value: {'copy_celery': True},
    TaskColumn.FROM_PLUGIN.value: {'copy_celery': True},
    TaskColumn.RESULTS.value: {'copy_celery': True},
    TaskColumn.TRACEBACK.value: {'copy_celery': True},
    TaskColumn.EXCEPTION.value: {'copy_celery': True},
    TaskColumn.LONG_NAME.value: {
        'copy_celery': True,
        'transform_celery': lambda e: {'name': e['long_name'].split('.')[-1]},
    },
    TaskColumn.NAME.value: {
        # TODO: firexapp should send long_name, since it will overwrite 'name' copied from celery. Then get rid of
        # the following config.
        'transform_celery': lambda e: {TaskColumn.NAME.value: e[TaskColumn.NAME.value].split('.')[-1],
                                       TaskColumn.LONG_NAME.value: e[TaskColumn.NAME.value]},
    },
    TaskColumn.CHAIN_DEPTH.value: {'copy_celery': True},
    TaskColumn.FIRST_STARTED.value: {'aggregate_keep_initial': True},
    'states': {'aggregate_merge': True},
    'url': {
        # TODO: only for backwards compat. Can use log_filepath.
        'transform_celery': lambda e: {TaskColumn.LOGS_URL.value: e['url']},
    },
    'log_filepath': {
        'transform_celery': lambda e: {TaskColumn.LOGS_URL.value: e['log_filepath']},
    },
    'local_received': {
        # Note first_started is never overwritten by aggregation.
        'transform_celery': lambda e: {TaskColumn.FIRST_STARTED.value: e['local_received']},
    },
}


def _get_keys_with_true(input_dict, key):
    return [k for k, v in input_dict.items() if v.get(key, False)]


DEFAULT_AGGREGATOR_CONFIG = EventAggregatorConfig(
    copy_fields=_get_keys_with_true(FIELD_CONFIG, 'copy_celery'),
    merge_fields=_get_keys_with_true(FIELD_CONFIG, 'aggregate_merge'),
    keep_initial_fields=_get_keys_with_true(FIELD_CONFIG, 'aggregate_keep_initial'),
    field_to_celery_transforms={k: v['transform_celery'] for k, v in FIELD_CONFIG.items() if 'transform_celery' in v},
)


def _deep_merge_keys(dict1, dict2, keys):
    dict1_to_merge = {k: v for k, v in dict1.items() if k in keys}
    dict2_to_merge = {k: v for k, v in dict2.items() if k in keys}
    return _deep_merge(dict1_to_merge, dict2_to_merge)


def _both_instance(o1, o2, _type):
    return isinstance(o1, _type) and isinstance(o2, _type)


def _deep_merge(dict1, dict2):
    result = dict(dict1)
    for d2_key in dict2:
        if d2_key in dict1:
            v1 = dict1[d2_key]
            v2 = dict2[d2_key]
            if _both_instance(v1, v2, dict):
                result[d2_key] = _deep_merge(v1, v2)
            elif _both_instance(v1, v2, list):
                result[d2_key] = v1 + v2
            elif _both_instance(v1, v2, set):
                result[d2_key] = v1.union(v2)
            elif v1 == v2:
                # already the same value in both dicts, take from either.
                result[d2_key] = v1
            else:
                # Both d1 and d2 have entries for d2_key, both entries are not dicts or lists or sets,
                # and the values are not the same. This is a conflict.
                # Overwrite d1's value to simulate dict.update() behaviour.
                result[d2_key] = v2
        else:
            # New key for d1, just add it.
            result[d2_key] = dict2[d2_key]
    return result


# Event data extraction/transformation without current state context.
def get_new_event_data(event, copy_fields, field_to_celery_transforms):
    new_task_data = {}
    for field in copy_fields:
        if field in event:
            new_task_data[field] = event[field]

    # Note if a field is both a copy field and a transform, the transform overrides if the output writes to the same
    # key.
    for field, transform in field_to_celery_transforms.items():
        if field in event:
            new_task_data.update(transform(event))

    return {event['uuid']: new_task_data}


def find_data_changes(task, new_task_data, keep_initial_fields, merge_fields):
    # Some fields overwrite whatever is present. Be permissive, since not all fields captured are from celery,
    # so not all have entries in the field config.
    no_overwrite_fields = keep_initial_fields + merge_fields
    override_dict = {k: v for k, v in new_task_data.items() if k not in no_overwrite_fields}

    changed_data = {}
    for new_data_key, new_data_val in override_dict.items():
        if new_data_key not in task or task[new_data_key] != new_data_val:
            changed_data[new_data_key] = new_data_val

    # Some field updates are dropped if there is already a value for that field name (keep initial).
    for no_overwrite_key in keep_initial_fields:
        if no_overwrite_key in new_task_data and no_overwrite_key not in task:
            changed_data[no_overwrite_key] = new_task_data[no_overwrite_key]

    # Some fields need to be accumulated across events, not overwritten from latest event.
    merged_values = _deep_merge_keys(task, new_task_data, merge_fields)
    for merged_data_key, merged_data_val in merged_values.items():
        if merged_data_key not in task or task[merged_data_key] != merged_data_val:
            changed_data[merged_data_key] = merged_data_val

    return changed_data


class FireXEventAggregator:
    """ Aggregates many events in to the task data model. """

    def __init__(self, aggregator_config: EventAggregatorConfig = DEFAULT_AGGREGATOR_CONFIG):
        self.tasks_by_uuid = {}
        self.new_task_num = 1
        self.root_uuid = None
        self.aggregator_config = aggregator_config

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
        terminal runstate. The 'task-incomplete' runstate is a fake (non-backend) terminal runstate
        that is generated here so that the UI can show a non-incomplete runstate.
        :return:
        """
        return [{'uuid': task['uuid'], 'type': 'task-incomplete', 'timestamp': datetime.now().timestamp()}
                for task in self.tasks_by_uuid.values()
                if task['state'] in INCOMPLETE_RUNSTATES]

    def is_root_complete(self):
        if not self.root_uuid or self.root_uuid not in self.tasks_by_uuid:
            return False  # Might not have root event yet.
        root_runstate = self.tasks_by_uuid[self.root_uuid].get('state', None)
        return root_runstate in COMPLETE_RUNSTATES

    def _get_or_create_task(self, task_uuid):
        if task_uuid not in self.tasks_by_uuid:
            task = {
                'uuid': task_uuid,
                'task_num': self.new_task_num,
            }
            self.new_task_num += 1
            self.tasks_by_uuid[task_uuid] = task
            is_new = True
        else:
            task = self.tasks_by_uuid[task_uuid]
            is_new = False
        return task, is_new

    def _aggregate_event(self, event):
        if ('uuid' not in event
                # The uuid can be null, it's unclear what this means but it can't be associated with a task.
                or not event['uuid']
                # Revoked events can be sent before any other, and we'll never get any data (name, etc) for that task.
                # Therefore ignore events that are for a new UUID that have revoked type.
                or (event['uuid'] not in self.tasks_by_uuid and event.get('type', '') == 'task-revoked')):
            return {}

        if event.get(TaskColumn.PARENT_ID.value, '__no_match') is None and self.root_uuid is None:
            self.root_uuid = event['uuid']

        new_data_by_task_uuid = get_new_event_data(event,
                                                   self.aggregator_config.copy_fields,
                                                   self.aggregator_config.field_to_celery_transforms)
        changes_by_task_uuid = {}
        for task_uuid, new_task_data in new_data_by_task_uuid.items():
            task, is_new_task = self._get_or_create_task(task_uuid)

            changed_data = find_data_changes(task,
                                             new_task_data,
                                             self.aggregator_config.keep_initial_fields,
                                             self.aggregator_config.merge_fields)
            task.update(changed_data)

            # If we just created the task, we need to send the auto-initialized fields, as well as data from the event.
            # If this isn't a new event, we only need to send what has changed.
            changes_by_task_uuid[task_uuid] = dict(task) if is_new_task else dict(changed_data)

        return changes_by_task_uuid
