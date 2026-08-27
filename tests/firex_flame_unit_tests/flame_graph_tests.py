import unittest

from firex_flame.flame_task_graph import _TaskFieldSentile, _FlameTask

basic_event = {
    'uuid': '1',
    'long_name': 'prefix.SomeTask',
    'type': 'task-started-info',
    'local_received': 0}
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

class _DummyModelDumper:
    def __init__(self):
        self._tasks_by_uuid = {}

    def dump_full_task(self, uuid, full_task_dict):
        self._tasks_by_uuid[uuid] = full_task_dict

    def load_full_task(self, uuid):
        return self._tasks_by_uuid.get(uuid)

def _create_task(uuid):
    return _FlameTask.create_task(uuid, task_num=1, model_dumper=None)

common_base_fields = {
    'task_num': 1,
    'has_completed': False,
    'was_revoked': False,
    'state': None,
}

class FlameGraphTest(unittest.TestCase):

    def test_basic_access(self):
        task = _create_task('1')
        self.assertEqual(task.get_uuid(), '1')
        self.assertEqual(task.get_field('firex_bound_args', None), None)
        self.assertEqual(
            task.get_full_task_dict(),
            common_base_fields | {
                'uuid': '1',
            }
        )

        firex_bound_args = {'field': '2'}
        task.update({'firex_bound_args': firex_bound_args})
        self.assertEqual(task.get_field('firex_bound_args'), firex_bound_args)

        task.update({'state': 'task-succeeded'})

        # verify preceeding operations do not touch the dumper, only set now that we need it.
        task.model_dumper = _DummyModelDumper()
        task.dump_full(set())

        # expect args unloaded.
        self.assertEqual(task._modelled.firex_bound_args, _TaskFieldSentile.UNLOADED)
        # reload unloaded field.
        self.assertEqual(task.get_field('firex_bound_args'), firex_bound_args)

        # expect args loaded
        self.assertEqual(
            task.get_full_task_dict(),
            common_base_fields | {
                'uuid': '1',
                'state': 'task-succeeded',
                'firex_bound_args': firex_bound_args,
            }
        )





