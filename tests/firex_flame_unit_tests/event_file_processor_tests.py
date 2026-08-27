import os
import unittest

from firex_flame.controller import FlameAppController
from firex_flame.event_file_processor import process_recording_file, get_model_or_rec_full_tasks_by_uuids

parent = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
test_data_dir = os.path.join(parent, "data")

def get_model_or_rec_full_task(logs_dir, uuid):
    task_by_uuid = get_model_or_rec_full_tasks_by_uuids(logs_dir, [uuid])
    return task_by_uuid[uuid]

class EventFileProcessorTests(unittest.TestCase):

    def test_process_rec_file(self):

        recording_file = os.path.join(test_data_dir, 'nop', 'firex_internal', 'flame', 'flame.rec')

        controller = FlameAppController({'logs_dir': test_data_dir})
        process_recording_file(controller, recording_file)

        self.assertEqual(2, len(controller.graph.get_full_tasks_by_uuid()), "Expected two tasks, root and nop.")
        # TODO: assert on metadata once args are emitted in celery events by firexapp.

    def test_rec_full_task(self):
        t = get_model_or_rec_full_task(os.path.join(test_data_dir, 'nop'), 'e33054ad-4a02-45ae-b9d3-f92eda6cdedd')
        self.assertEqual('nop', t['name'])
