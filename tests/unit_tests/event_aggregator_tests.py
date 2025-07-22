import unittest
from types import MappingProxyType

from firexapp.events.event_aggregator import FireXEventAggregator

basic_event = MappingProxyType(
    {'uuid': '1', 'long_name': 'prefix.SomeTask', 'type': 'task-started', 'local_received': 0})
basic_event_added_fields = {
    'state': basic_event['type'],
    'task_num': 1,
    'name': 'SomeTask',
    'states': [{
        'state': basic_event['type'],
        'timestamp': basic_event['local_received'],
    }],
}


class EventAggregatorTests(unittest.TestCase):

    def test_add_new_task(self):
        aggregator = FireXEventAggregator()

        aggregator.aggregate_events([basic_event])

        expected_task = basic_event | basic_event_added_fields
        expected_task['first_started'] = expected_task.pop('local_received')
        self.assertEqual({expected_task['uuid']: expected_task}, aggregator.tasks_by_uuid)

    def test_ignore_missing_uuid(self):
        aggregator = FireXEventAggregator()

        event = dict(basic_event)
        event.pop('uuid')

        aggregator.aggregate_events([event])
        self.assertEqual({}, aggregator.tasks_by_uuid)

    def test_no_copy_unknown_field(self):
        aggregator = FireXEventAggregator()

        event = dict(basic_event)
        unknown_key = '__fake__field'
        event[unknown_key] = 'value'

        aggregator.aggregate_events([event])
        self.assertTrue(unknown_key not in aggregator.tasks_by_uuid[event['uuid']])

    def test_states_aggregated(self):
        aggregator = FireXEventAggregator()

        event2 = basic_event | dict(type='task-blocked', local_received=1)

        aggregator.aggregate_events([basic_event, event2])
        expected_states = [
            {
                'state': basic_event['type'],
                'timestamp': basic_event['local_received']},
            {
                'state': event2['type'],
                'timestamp': event2['local_received']},
        ]
        self.assertEqual(
            expected_states,
            aggregator.tasks_by_uuid[basic_event['uuid']]['states'])

    def test_aggregate_states(self):
        aggregator = FireXEventAggregator()

        event1 = dict(basic_event)
        events = [
            event1,
            {'uuid': event1['uuid'], 'type': 'task-blocked', 'local_received': 1},
            {'uuid': event1['uuid'], 'type': 'task-started', 'local_received': 2},
            {'uuid': event1['uuid'], 'type': 'task-succeeded', 'local_received': 3},
        ]

        aggregator.aggregate_events(events)

        aggregated_states = aggregator.tasks_by_uuid[event1['uuid']]['states']
        expected_states = [
            {
                'state': e['type'],
                'timestamp': e['local_received']
            } for e in events
        ]
        self.assertEqual(expected_states, aggregated_states)

    def test_capture_root(self):
        aggregator = FireXEventAggregator()

        event1 = {'parent_id': None} | basic_event
        event2 = {**event1, 'parent_id': event1['uuid'], 'uuid': 2}

        aggregator.aggregate_events([event1, event2])
        self.assertEqual(event1['uuid'], aggregator.root_uuid)
