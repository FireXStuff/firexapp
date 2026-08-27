import os
import gzip
import json

from firex_flame.model_dumper import find_flame_model_dir, get_model_full_tasks_by_names, \
    index_tasks_by_names, get_full_tasks_by_slim_pred
from firex_flame.flame_task_graph import TASK_ARGS
from firex_flame.flame_helper import find_rec_file, find
from firex_flame.controller import FlameAppController


def load_events_from_rec_file(recording_file):
    assert os.path.isfile(recording_file), f"Recording file doesn't exist: {recording_file}"

    real_rec = os.path.realpath(recording_file)
    if real_rec.endswith('.gz'):
        with gzip.open(real_rec, 'rt', encoding='utf-8') as rec:
            event_lines = rec.readlines()
    else:
        with open(recording_file) as rec:
            event_lines = rec.readlines()

    event_count = 0
    for event_line in event_lines:
        if event_line:
            event =  json.loads(event_line)
            yield event
            event_count += 1
            if event_count % 100 == 0:
                print(f"Processed event {event_count} with uuid {event.get('uuid')}")


def process_recording_file(flame_controller: FlameAppController, recording_file: str):

    for event in load_events_from_rec_file(recording_file):
        flame_controller.update_graph_and_sio_clients([event])

    if flame_controller.is_root_complete():
        # Kludge incomplete runstates that will never become terminal.
        flame_controller.finalize_all_tasks()

    if not flame_controller.run_metadata.get('uid'):
        root_task = flame_controller.graph.get_root_task()
        if root_task:
            flame_controller.run_metadata['uid'] = find([TASK_ARGS, 'uid'], root_task)
            flame_controller.run_metadata['chain'] = find([TASK_ARGS, 'chain'], root_task)


def get_tasks_from_rec_file(log_dir=None, rec_filepath=None, mark_incomplete=False):
    assert bool(log_dir) ^ bool(rec_filepath), "Need exclusively either log directory of rec_file path."
    if not rec_filepath:
        rec_file = find_rec_file(log_dir)
    else:
        rec_file = rec_filepath
    assert os.path.exists(rec_file), f"Recording file not found: {rec_file}"
    flame_controller = FlameAppController({'logs_dir': log_dir})
    process_recording_file(flame_controller, rec_file)

    if mark_incomplete:
        flame_controller.finalize_all_tasks()

    return flame_controller.graph.get_full_tasks_by_uuid(), flame_controller.graph.root_uuid


def get_model_or_rec_full_tasks_by_names(logs_dir, task_names):
    if os.path.isdir(find_flame_model_dir(logs_dir)):
        return get_model_full_tasks_by_names(logs_dir, task_names)

    rec_file = find_rec_file(logs_dir)
    if os.path.isfile(rec_file):
        tasks_by_uuid, _ = get_tasks_from_rec_file(rec_filepath=rec_file)
        return index_tasks_by_names(tasks_by_uuid.values(), task_names)

    raise Exception("Found neither model directory or rec_file, no source of task data in: %s" % logs_dir)


def get_model_or_rec_full_tasks_by_uuids(logs_dir, uuids):
    if os.path.isdir(find_flame_model_dir(logs_dir)):
        return get_full_tasks_by_slim_pred(logs_dir, lambda st: st['uuid'] in uuids)

    if os.path.isdir(logs_dir):
        tasks_by_uuid, _ = get_tasks_from_rec_file(log_dir=logs_dir)
        return {u: t for u, t in tasks_by_uuid.items() if u in uuids}

    raise Exception("Found neither model directory or rec_file, no source of task data in: %s" % logs_dir)
