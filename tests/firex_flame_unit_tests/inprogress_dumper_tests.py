import os
import unittest
import tempfile
from pathlib import Path
import json

from firex_flame.controller import FlameAppController, RunningModelDumper
from firex_flame.model_dumper import load_slim_tasks, get_full_task_path, get_tasks_slim_file, load_full_task, \
    _get_base_model_dir
from firex_flame.flame_helper import wait_until_path_exist

import gevent

test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


class TaskQueryTests(unittest.TestCase):

    def test_write_only_slim(self):
        with tempfile.TemporaryDirectory() as log_dir:
            uuid = '1'
            controller = FlameAppController({'logs_dir': log_dir})
            controller.update_graph_and_sio_clients([{'uuid': uuid, 'name': 'hello'}])
            running_model_dumper = controller.running_dumper_queue
            running_model_dumper.queue_write_slim()
            running_model_dumper._queue.join()

            loaded_slim_tasks = load_slim_tasks(log_dir)
            self.assertEqual(controller.graph.get_slim_tasks_by_uuid(), loaded_slim_tasks)
            self.assertFalse(os.path.exists(get_full_task_path(log_dir, uuid)))

    def test_write_only_tasks(self):
        with tempfile.TemporaryDirectory() as log_dir:
            uuid1 = '1'
            uuid2 = '2'
            uuid3 = '3'
            events = [
                {'uuid': uuid1, 'name': 'hello', 'flame_data': 'some_data'},
                {'uuid': uuid2, 'name': 'hello again', 'flame_data': 'some other data'},
                {'uuid': uuid3, 'name': 'another name', 'flame_data': 'some other data'},
            ]
            controller = FlameAppController({'logs_dir': log_dir})
            controller.graph.update_graph_from_celery_events(events)
            running_model_dumper = controller.running_dumper_queue

            # Confirm writing a non-slim field does not cause the slim tasks to be written.
            controller.update_graph_and_sio_clients(
                [{'uuid': uuid1, 'external_commands': {'key': 1}}]
            )
            running_model_dumper._queue.join()
            self.assertFalse(os.path.exists(get_tasks_slim_file(log_dir)))

            controller.update_graph_and_sio_clients(
                [
                    {'uuid': uuid1, 'type': 'task-started-info'},
                    {'uuid': uuid2, 'type': 'task-succeeded'},
                    {'uuid': uuid3, 'type': 'task-blocked'}, # this dumper will not write this task.
                ]
            )
            running_model_dumper._queue.join()
            self.assertTrue(os.path.exists(get_tasks_slim_file(log_dir)))

            self.assertEqual(controller.graph.get_full_task_dict(uuid1), load_full_task(log_dir, uuid1))
            self.assertEqual(controller.graph.get_full_task_dict(uuid2), load_full_task(log_dir, uuid2))

            # uuid3 not dumped due to event_type='task-blocked'
            self.assertFalse(os.path.exists(get_full_task_path(log_dir, uuid3)))

            # Expect uuid to be remaining, written on finalize_all_tasks.
            controller.finalize_all_tasks()
            self.assertEqual(controller.graph.get_full_task_dict(uuid3), load_full_task(log_dir, uuid3))

    def test_always_write_full_task_after_completed(self):
        with tempfile.TemporaryDirectory() as log_dir:
            uuid1 = '1'
            initial_task = {'uuid': uuid1, 'name': 'hello', 'flame_data': 'some_data'}

            controller = FlameAppController({'logs_dir': log_dir})
            controller.graph.update_graph_from_celery_events([initial_task])
            running_model_dumper = controller.running_dumper_queue
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-started-info'})

            running_model_dumper._queue.join()
            self.assertEqual(controller.graph.get_full_task_dict(uuid1), load_full_task(log_dir, uuid1))

            first_update = {'more_data': 1}
            controller.graph.update_graph_from_celery_events([first_update])
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-blocked'})
            running_model_dumper._queue.join()
            # Expect data unchanged due to task-blocked.
            self.assertNotIn('more_data', load_full_task(log_dir, uuid1))

            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-completed'})
            running_model_dumper._queue.join()
            self.assertEqual(controller.graph.get_full_task_dict(uuid1), load_full_task(log_dir, uuid1))

            second_updated_task = {'other_more_data': 2}
            controller.graph.update_graph_from_celery_events([second_updated_task])
            running_model_dumper.queue_maybe_write_tasks({uuid1: 'task-blocked'})
            running_model_dumper._queue.join()
            # After a task-completed, task-blocked will cause updates.
            self.assertEqual(
                controller.graph.get_full_task_dict(uuid1),
                load_full_task(log_dir, uuid1))

    def test_dump_extra_task_representations(self):
        with tempfile.TemporaryDirectory() as log_dir:
            extra_repr = Path(log_dir, 'extra-task-repr-query.json')

            model_file_name = "extra-repr_tasks.json"
            extra_repr.write_text(
                json.dumps(
                    {
                        "model_file_name": model_file_name,
                        "task_queries": [
                            {
                                "matchCriteria": { "type": "always-select-fields"},
                                "selectPaths": ["uuid", "name"]
                            },
                            {
                                "matchCriteria": {
                                    "type": "equals",
                                    "value": {"name": "hello"}
                                },
                                "selectDescendants": [
                                    {
                                        "type": "equals",
                                        "value": {"name": "hello_child"}
                                    },
                                ],
                            },
                        ]
                    }
                ),
                encoding='utf-8',
            )

            uuid1 = '1'
            # the model dumper writes extra task representations periodically.
            controller = FlameAppController(
                {'logs_dir': log_dir},
                extra_task_representations=[str(extra_repr)],
                min_age_repr_dump=0, # disable dump age throttling.
            )
            controller.update_graph_and_sio_clients(
                [{'uuid': uuid1, 'name': 'hello', 'flame_data': 'some_data', 'parent_id': None, 'type': 'task-started-info'}]
            )
            controller.running_dumper_queue._queue.join()

            extra_repr_tasks = Path(_get_base_model_dir(log_dir), model_file_name)
            model_file_exists = wait_until_path_exist(str(extra_repr_tasks))
            self.assertTrue(model_file_exists)

            first_tasks_repr = json.loads(extra_repr_tasks.read_text())
            expected_task_repr = {uuid1: {'uuid': uuid1, 'name': 'hello'}}
            self.assertEqual(expected_task_repr, first_tasks_repr)

            # double update necessary till aggregator moved in to controller.
            controller.update_graph_and_sio_clients(
                [
                    {'uuid': '2', 'name': 'hello_child', 'parent_id': uuid1, 'type': 'task-started-info'}
                ]
            )
            controller.running_dumper_queue._queue.join()

            expected_task_repr[uuid1]['descendants'] = {'2': {'name': 'hello_child', 'uuid': '2'}}
            second_tasks_repr = json.loads(extra_repr_tasks.read_text())
            self.assertEqual(expected_task_repr, second_tasks_repr)

    def test_partial_descedant_updates_ancestor(self):
        #
        # when a -> b -> c graph is updated, make sure when c changes,
        # a's descendant query for it is updated properly.
        #

        with tempfile.TemporaryDirectory() as log_dir:
            extra_repr = Path(log_dir, 'a-b-c-query.json')

            model_file_name = "a-b-c-tasks.json"
            extra_repr.write_text(
                json.dumps(
                    {
                        "model_file_name": model_file_name,
                        "task_queries": [
                            {
                                "matchCriteria": { "type": "always-select-fields"},
                                "selectPaths": ["uuid", "name"]
                            },
                            {
                                "matchCriteria": {
                                    "type": "equals",
                                    "value": {"name": "a"},
                                },
                                "selectPaths": ["flame_data"],
                                "selectDescendants": [
                                    {
                                        "type": "equals",
                                        "value": {"name": "b"}
                                    },
                                    {
                                        "type": "equals",
                                        "value": {"name": "c"}
                                    },
                                ],
                            },
                            {
                                "matchCriteria": {
                                    "type": "equals",
                                    "value": {"name": "a"},
                                },
                                "selectPaths": ["firex_bound_args"],
                                "selectDescendants": [
                                    {
                                        "type": "equals",
                                        "value": {"name": "c"}
                                    },
                                ],
                            },
                        ]
                    }
                ),
                encoding='utf-8',
            )

            uuid1 = '1'
            # the model dumper writes extra task representations periodically.
            controller = FlameAppController(
                {'logs_dir': log_dir},
                extra_task_representations=[str(extra_repr)],
                min_age_repr_dump=0, # disable dump age throttling.
            )
            controller.update_graph_and_sio_clients(
                [
                    {'uuid': uuid1, 'name': 'a', 'parent_id': None, 'type': 'task-started-info'},
                    {'uuid': '2', 'name': 'b', 'parent_id': '1', 'type': 'task-started-info'},
                    {
                        'uuid': '3',
                        'name': 'c',
                        'parent_id': '2',
                        'type': 'task-started-info',
                        'flame_data': {'1': 'a'},
                        'firex_bound_args': ['arg'],
                    },
                ],
            )
            controller.running_dumper_queue._queue.join()

            task_model = controller.query_full_tasks(
                task_queries=None,
                model_file_name=model_file_name,
                force=True,
            )

            expected_query_result = {
                '1': {
                    'name': 'a', 'uuid': '1',
                    'descendants': {
                        '2': {'name': 'b', 'uuid': '2'},
                        '3': {
                            'name': 'c',
                            'uuid': '3',
                            'flame_data': {'1': 'a'},
                            'firex_bound_args': ['arg'],
                        }
                    }
                }
            }

            self.assertEqual(task_model, expected_query_result)

            controller.update_graph_and_sio_clients(
                [
                    {
                        'uuid': '3', 'name': 'c', 'parent_id': '2',
                        'type': 'task-flame-data',
                        'flame_data': {'2': 'b'},
                    },
                ],
            )
            controller.running_dumper_queue._queue.join()

            latest_model = controller.query_full_tasks(
                model_file_name=model_file_name,
                task_queries=None,
                force=True,
            )

            expected_query_result['1']['descendants']['3']['flame_data']['2'] = 'b'
            self.assertEqual(
                latest_model,
                expected_query_result,
            )
