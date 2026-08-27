import json
import logging
import os
from pathlib import Path
import tempfile
from typing import Optional
from gevent.fileobject import FileObject

from firexapp.common import wait_until
from firex_flame.flame_helper import get_flame_debug_dir

logger = logging.getLogger(__name__)


def atomic_write_json(filename, data):
    filename = os.path.realpath(filename)
    filedir = os.path.dirname(filename)
    with tempfile.NamedTemporaryFile(mode='w',
                                     dir=filedir,
                                     delete=False) as f:
        json.dump(
            data,
            fp=FileObject(f, 'w'), # closes fd
            sort_keys=True,
            indent=2)

        os.chmod(f.name, 0o664)
        os.replace(f.name, filename)


def get_flame_model_dir(firex_logs_dir) -> str:
    return os.path.join(get_flame_debug_dir(firex_logs_dir), 'model')


def get_flame_old_model_dir(firex_logs_dir):
    return os.path.join(firex_logs_dir, 'flame_model')


# Temporarily support both current and old model directory locations.
def find_flame_model_dir(firex_logs_dir):
    model_dir = get_flame_model_dir(firex_logs_dir)
    if os.path.isdir(model_dir):
        return model_dir
    return get_flame_old_model_dir(firex_logs_dir)


def _get_base_model_dir(firex_logs_dir=None, root_model_dir=None):
    assert bool(firex_logs_dir) ^ bool(root_model_dir), \
        "Need exclusively either logs dir or root model dir."
    if firex_logs_dir:
        return find_flame_model_dir(firex_logs_dir)
    else:
        return root_model_dir


def get_all_tasks_dir(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'full-tasks')


def get_run_metadata_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'run-metadata.json')


def get_run_metadata(firex_logs_dir=None, root_model_dir=None):
    run_metadata_file = get_run_metadata_file(firex_logs_dir, root_model_dir)
    if os.path.isfile(run_metadata_file):
        return json.loads(Path(run_metadata_file).read_text())
    return None


def legacy_is_flame_revoked(firex_logs_dir) -> bool:
    run_metadata = get_run_metadata(firex_logs_dir)
    if run_metadata:
        return bool(run_metadata.get('revoke_timestamp'))
    return False


def _get_flame_url(firex_logs_dir) -> Optional[str]:
    metadata = get_run_metadata(firex_logs_dir)
    if metadata:
        return metadata.get('flame_url')
    return None


def wait_and_get_flame_url(firex_logs_dir: str, timeout=15, sleep_for=0.5) -> Optional[str]:
    return wait_until(
        _get_flame_url,
        timeout=timeout,
        sleep_for=sleep_for,
        firex_logs_dir=firex_logs_dir,
    )


def get_tasks_slim_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'slim-tasks.json')


def get_model_complete_file(firex_logs_dir=None, root_model_dir=None):
    return os.path.join(_get_base_model_dir(firex_logs_dir, root_model_dir), 'model-complete')


def load_slim_tasks(log_dir):
    return json.loads(Path(get_tasks_slim_file(log_dir)).read_text())


def socketio_client_task_queries_request(flame_url, task_queries):
    import socketio
    sio_client = socketio.Client()
    sio_client.connect(flame_url)
    try:
        resp = {'tasks_by_uuid': None}
        @sio_client.on('graph-state')
        def my_event(data):
            resp['tasks_by_uuid'] = data

        sio_client.call('send-graph-state', {'task_queries': task_queries})
        assert resp['tasks_by_uuid'] is not None
    finally:
        sio_client.disconnect()

    return resp['tasks_by_uuid']


def load_task_representation(firex_logs_dir, representation_file, consider_running=False):
    with open(representation_file) as fp:
            representation_data = json.load(fp)

    task_rep_file = os.path.join(_get_base_model_dir(firex_logs_dir=firex_logs_dir),
                                 representation_data['model_file_name'])
    # TODO: this assumes the task representation file is only present once it's complete!
    #   If dumping is moved to occur during event reception, this will need to consider run_metadata.is_complete.
    if os.path.isfile(task_rep_file):
        # If file has already been dumped, just load it.
        with open(task_rep_file) as fp:
            return json.load(fp)
    elif consider_running:
        # If file hasn't been dumped, try to query a running flame server.
        run_metadata_file = get_run_metadata_file(firex_logs_dir=firex_logs_dir)
        if os.path.isfile(run_metadata_file):
            with open(run_metadata_file) as fp:
                run_metadata = json.load(fp)
            return socketio_client_task_queries_request(run_metadata['flame_url'],
                                                        representation_data['task_queries'])

    return None


def get_full_task_path(logs_dir, task_uuid):
    return os.path.join(get_all_tasks_dir(firex_logs_dir=logs_dir), task_uuid + '.json')


def load_full_task(log_dir, task_uuid):
    return json.loads(Path(get_full_task_path(log_dir, task_uuid)).read_text())


def index_tasks_by_names(tasks, names):
    tasks_by_name = {n: [] for n in names}
    for t in tasks:
        if t['name'] in names:
            tasks_by_name[t['name']].append(t)
    return tasks_by_name


def get_model_full_tasks_by_names(log_dir, task_names):
    if isinstance(task_names, str):
        task_names = [task_names]
    tasks_by_uuid = get_full_tasks_by_slim_pred(log_dir, lambda st: st.get('name', None) in task_names)
    return index_tasks_by_names(tasks_by_uuid.values(), task_names)


def get_model_slim_tasks_by_names(log_dir, task_names):
    if isinstance(task_names, str):
        task_names = [task_names]
    tasks_by_uuid = get_slim_tasks_by_slim_pred(
        log_dir,
        lambda st: st.get('name') in task_names)
    return index_tasks_by_names(tasks_by_uuid.values(), task_names)

def get_full_tasks_by_slim_pred(log_dir, slim_predicate):
    return {
        uuid: load_full_task(log_dir, uuid)
        for uuid in get_slim_tasks_by_slim_pred(log_dir, slim_predicate)
    }

def get_slim_tasks_by_slim_pred(log_dir, slim_predicate):
    return {
        uuid: slim_task
        for uuid, slim_task in load_slim_tasks(log_dir).items()
        if slim_predicate(slim_task)
    }


def is_dump_complete(firex_logs_dir):
    return os.path.exists(get_model_complete_file(firex_logs_dir=firex_logs_dir))

