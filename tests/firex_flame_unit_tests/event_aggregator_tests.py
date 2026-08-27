import unittest

from firex_flame.flame_task_graph import FlameEventAggregator, FlameTaskGraph, _TaskFieldSentile

basic_event = {
    'uuid': '1',
    'long_name': 'prefix.SomeTask',
    'type': 'task-started-info',
    'local_received': 0,
}
basic_event_added_fields = {
    'state': basic_event['type'],
    'task_num': 1,
    'name': 'SomeTask',
    'first_started': basic_event['local_received'],
    'latest_timestamp': basic_event['local_received'],
    'was_revoked': False,
    'has_completed': False,
    'states': [{'state': basic_event['type'], 'timestamp': basic_event['local_received']}],
}

def _norm_state(state: str):
    return 'task-started' if state == 'task-started-info' else state


def _to_expected_task(task: dict):
    task['state'] = _norm_state(task['state'])
    if 'states' in task:
        for s in task['states']:
            s['state'] = _norm_state(s['state'])
    return task

class EventAggregatorTests(unittest.TestCase):

    def test_add_new_task(self):
        aggregator = FlameEventAggregator({})

        aggregator.aggregate_events([basic_event])

        expected_task = _to_expected_task(basic_event | basic_event_added_fields)
        expected_task.pop('local_received')
        self.assertEqual(
            {expected_task['uuid']: expected_task},
            {u: t.as_dict() for u, t in aggregator._tasks_by_uuid.items()},
        )

    def test_ignore_missing_uuid(self):
        aggregator = FlameEventAggregator({})

        event = dict(basic_event)
        event.pop('uuid')

        aggregator.aggregate_events([event])
        self.assertEqual({}, aggregator._tasks_by_uuid)

    def test_no_copy_unknown_field(self):
        aggregator = FlameEventAggregator({})

        event = dict(basic_event)
        unknown_key = '__fake__field'
        event[unknown_key] = 'value'

        aggregator.aggregate_events([event])
        self.assertEqual(
            aggregator._tasks_by_uuid[event['uuid']].get_field(unknown_key),
            _TaskFieldSentile.UNSET,
        )

    def test_merge_flame_data(self):
        aggregator = FlameEventAggregator({})

        event1 = dict(basic_event)
        event1['flame_data'] = {
            'k1': {'k2': ['v1']},
        }
        event2 = {
            'uuid': event1['uuid'],
            'flame_data': {
                'k1': {'k2': ['v2']},
            }
        }
        expected_final_flame_data = {'k1': {'k2': ['v1', 'v2']}}

        aggregator.aggregate_events([event1])
        changed_data = aggregator.aggregate_events([event2])

        self.assertEqual({event1['uuid']: {'flame_data': expected_final_flame_data}}, changed_data)
        self.assertEqual(
            expected_final_flame_data,
            aggregator._tasks_by_uuid[event1['uuid']].get_field('flame_data'))

    def test_aggregate_states(self):
        aggregator = FlameEventAggregator({})

        event1 = dict(basic_event)
        events = [
            event1,
            {'uuid': event1['uuid'], 'type': 'task-blocked', 'local_received': 1},
            {'uuid': event1['uuid'], 'type': 'task-started-info', 'local_received': 2},
            {'uuid': event1['uuid'], 'type': 'task-succeeded', 'local_received': 3},
        ]
        aggregator.aggregate_events(events)

        aggregated_states = aggregator._tasks_by_uuid[event1['uuid']].get_field('states')
        expected_states = [
            _to_expected_task({
                'state': e['type'],
                'timestamp': e['local_received']
            })
            for e in events]
        self.assertEqual(expected_states, aggregated_states)

    def test_capture_root(self):
        graph  = FlameTaskGraph({})
        event1 = {'parent_id': None, **basic_event}
        event2 = {**event1, 'parent_id': event1['uuid'], 'uuid': 2}

        graph.update_graph_from_celery_events([event1, event2])
        self.assertEqual(event1['uuid'], graph.root_uuid)

    def test_states_aggregated(self):
        aggregator = FlameEventAggregator({})

        event2 = basic_event | dict(type='task-blocked', local_received=1)

        aggregator.aggregate_events([basic_event, event2])
        expected_states = [
            _to_expected_task(
                {'state': basic_event['type'], 'timestamp': basic_event['local_received']}
            ),
            _to_expected_task(
                {'state': event2['type'], 'timestamp': event2['local_received']}
            ),
        ]
        self.assertEqual(
            expected_states,
            aggregator._tasks_by_uuid[basic_event['uuid']].get_field('states'),
        )

    def test_aggregate_non_celery_field(self):
        aggregator = FlameEventAggregator({})
        event1 = basic_event | {'url': 'some_url'}

        aggregator.aggregate_events([event1])
        # The rule for the celery key 'url' creates a new key 'logs_url'. This test verifies non-celery keys are
        # propagated.
        self.assertEqual('some_url', aggregator._tasks_by_uuid[basic_event['uuid']].get_field('logs_url'))

    def test_late_root_uuid(self):
        graph  = FlameTaskGraph({})

        root_id = "b954db0d-e308-4cd4-bcd9-dbbed3982067"
        flame_data = {'key': 'value'}
        graph.update_graph_from_celery_events([
            {"freq": 5, "sw_ident": "py-celery", "sw_ver": "9.9.9", "sw_sys": "Linux", "timestamp": 1670605159.61279, "type": "worker-heartbeat", "local_received": 1670605159.613447},
            {
                "uuid": "92b47986-7c9e-47b8-8949-bf3c047fd726", "name": "firexapp.submit.report_trigger.RunInitialReport",
                "root_id": root_id,
                "parent_id": "b954db0d-e308-4cd4-bcd9-dbbed3982067",
                "timestamp": 1670605164.6179929, "type": "task-received", "local_received": 1670605164.6221592},
            {
                "utcoffset": 8, "pid": 44987, "clock": 1, "uuid": "b954db0d-e308-4cd4-bcd9-dbbed3982067", "timestamp": 1670605164.6242092, "type": "task-started-info", "local_received": 1670605164.6256702},
            {"uuid": "92b47986-7c9e-47b8-8949-bf3c047fd726", "timestamp": 1670605164.6301577, "type": "task-started-info", "local_received": 1670605164.6447308},
            {"uuid": "b954db0d-e308-4cd4-bcd9-dbbed3982067", "timestamp": 1670605164.6375537, "type": "task-send-flame", 'flame_data': flame_data, "local_received": 1670605164.646939},
        ])

        self.assertEqual(root_id, graph.root_uuid)
        # Make sure subsequent events to root ID are aggregated.
        self.assertEqual(flame_data, graph.get_root_task()['flame_data'])