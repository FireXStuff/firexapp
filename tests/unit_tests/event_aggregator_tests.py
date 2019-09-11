import unittest

from firexapp.events.event_aggregator import FireXEventAggregator

basic_event = {'uuid': '1', 'long_name': 'prefix.SomeTask', 'type': 'task-started', 'timestamp': 0}
basic_event_added_fields = {
            'state': basic_event['type'],
            'task_num': 1,
            'name': 'SomeTask',
            'states': [{'state': basic_event['type'], 'timestamp': basic_event['timestamp']}],
        }


class EventAggregatorTests(unittest.TestCase):

    def test_add_new_task(self):
        aggregator = FireXEventAggregator()

        aggregator.aggregate_events([basic_event])

        expected_task = {**basic_event, **basic_event_added_fields}
        expected_task.pop('timestamp')
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

        event2 = {**basic_event,
                  'type': 'task-blocked',
                  'timestamp': 1,
                  }

        aggregator.aggregate_events([basic_event, event2])
        expected_states = [
            {'state': basic_event['type'], 'timestamp': basic_event['timestamp']},
            {'state': event2['type'], 'timestamp': event2['timestamp']},
        ]
        self.assertEqual(expected_states, aggregator.tasks_by_uuid[basic_event['uuid']]['states'])

    def test_aggregate_states(self):
        aggregator = FireXEventAggregator()

        event1 = dict(basic_event)
        events = [
            event1,
            {'uuid': event1['uuid'], 'type': 'task-blocked', 'timestamp': 1},
            {'uuid': event1['uuid'], 'type': 'task-started', 'timestamp': 2},
            {'uuid': event1['uuid'], 'type': 'task-succeeded', 'timestamp': 3},
        ]

        aggregator.aggregate_events(events)

        aggregated_states = aggregator.tasks_by_uuid[event1['uuid']]['states']
        expected_states = [{'state': e['type'], 'timestamp': e['timestamp']} for e in events]
        self.assertEqual(expected_states, aggregated_states)

    def test_capture_root(self):
        aggregator = FireXEventAggregator()

        event1 = {'parent_id': None, **basic_event}
        event2 = {**event1, 'parent_id': event1['uuid'], 'uuid': 2}

        aggregator.aggregate_events([event1, event2])
        self.assertEqual(event1['uuid'], aggregator.root_uuid)
