import json
import logging
import os
from pathlib import Path
import psutil
import time
import signal
from dataclasses import dataclass
from typing import Optional, List, Union
import hashlib
import platform
import socket

from firexapp.submit.uid import Uid

logger = logging.getLogger(__name__)

DEFAULT_FLAME_TIMEOUT = 60 * 60 * 24 * 2
REVOKE_REASON_KEY = 'revoke_reason'


@dataclass(frozen=True)
class FlameServerConfig:
    webapp_port: int
    serve_logs_dir: bool
    recording_file: Optional[str]
    extra_task_dump_paths: List[str]
    authed_user_request_path: List[str]


def get_flame_redirect_file_path(root_logs_dir):
    return os.path.join(root_logs_dir, 'flame.html')


def get_flame_debug_dir(root_logs_dir):
    return os.path.join(root_logs_dir, Uid.debug_dirname, 'flame')


def get_flame_pid_file_path(root_logs_dir):
    return os.path.join(get_flame_debug_dir(root_logs_dir), 'flame.pid')


def get_flame_pid(root_logs_dir):
    return int(Path(get_flame_pid_file_path(root_logs_dir)).read_text().strip())


def wait_until(predicate, timeout, sleep_for, *args, **kwargs):
    max_time = time.time() + timeout
    while time.time() < max_time:
        pred_result = predicate(*args, **kwargs)
        if pred_result:
            return pred_result
        time.sleep(sleep_for)
    return predicate(*args, **kwargs)


def wait_until_pid_not_exist(pid, timeout=7, sleep_for=1):
    return wait_until(lambda p: not psutil.pid_exists(p), timeout, sleep_for, pid)


def web_request_ok(url):
    import requests
    try:
        return requests.get(url).ok
    except requests.exceptions.ConnectionError:
        return False


def wait_until_web_request_ok(url, timeout=10, sleep_for=1):
    return wait_until(web_request_ok, timeout, sleep_for, url)


def wait_until_path_exist(path, timeout=7, sleep_for=1):
    return wait_until(os.path.exists, timeout, sleep_for, path)


def json_file_fn(json_file_path, fn):
    if not os.path.isfile(json_file_path):
        return False
    try:
        file_data = json.loads(Path(json_file_path).read_text())
    except (json.decoder.JSONDecodeError, OSError):
        return False
    else:
        return fn(file_data)


def get_rec_file(log_dir):
    return os.path.join(get_flame_debug_dir(log_dir), 'flame.rec')


def find_rec_file(log_dir):
    # Formerly was used for backwards compatability, now an alias for get_rec_file
    return get_rec_file(log_dir)

def get_hostname():
    myplatform = platform.system()
    myhostname = socket.gethostname()
    myhostname = f"{myhostname}.local" if myplatform== "Darwin" and not myhostname.endswith("local") else myhostname
    return myhostname


def get_flame_url_from_port(port: int) -> str:
    return f'http://{get_hostname()}:{int(port)}'


class PathNotFoundException(Exception):
    pass


def find(keys, input_dict, raise_error=False):
    result = input_dict
    for key in keys:
        try:
            result = result[key]
        except Exception:
            if raise_error:
                raise PathNotFoundException()
            return None
    return result


def filter_paths(input_dict, paths_to_values):
    results = {}
    for in_key, in_vals in input_dict.items():
        results[in_key] = []
        for in_val in in_vals:
            matches_all = all(to_equal == find(p, in_val) for p, to_equal in paths_to_values.items())
            if matches_all:
                results[in_key].append(in_val)
    return results


def kill_flame(log_dir, sig=signal.SIGKILL, timeout=10):
    flame_pid = get_flame_pid(log_dir)
    kill_and_wait(flame_pid, sig, timeout)
    return flame_pid


def kill_and_wait(pid, sig=signal.SIGKILL, timeout=10):
    if psutil.pid_exists(pid):
        os.kill(pid, sig)
        wait_until_pid_not_exist(pid, timeout=timeout)
    return not psutil.pid_exists(pid)


def create_rel_symlink(existing_path, symlink, target_is_directory=False):
    rel_new_file = os.path.relpath(existing_path, start=os.path.dirname(symlink))
    os.symlink(rel_new_file, symlink, target_is_directory=target_is_directory)


class BrokerConsumerConfig:

    def __init__(self, max_retry_attempts, receiver_ready_file, terminate_on_complete):
        self.max_retry_attempts = max_retry_attempts
        self.receiver_ready_file = receiver_ready_file
        self.terminate_on_complete = terminate_on_complete


def is_json_file(file_path):
    try:
        json.loads(Path(file_path).read_text())
    except json.decoder.JSONDecodeError:
        return False
    else:
        return True


def _both_instance(o1, o2, _type):
    return isinstance(o1, _type) and isinstance(o2, _type)


def deep_merge(container1: Union[dict,list,set], container2: Union[dict,list,set]) -> dict:
    if _both_instance(container1, container2, list):
        # TODO: could deep merge nested dicts.
        return container1 + container2
    elif _both_instance(container1, container2, set):
        return container1.union(container2)
    elif _both_instance(container1, container2, (str, float, int, bool)):
        return container2

    dict1 = container1
    dict2 = container2
    result = dict(dict1)

    for d2_key in dict2:
        if d2_key in dict1:
            v1 = dict1[d2_key]
            v2 = dict2[d2_key]
            if _both_instance(v1, v2, dict):
                result[d2_key] = deep_merge(v1, v2)
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

def flatten(l):
    return [item for sublist in l for item in sublist]

def get_dict_json_md5(query_config):
    return hashlib.md5(json.dumps(query_config, sort_keys=True).encode('utf-8')).hexdigest()
