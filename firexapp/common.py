import time

import os
import psutil
import re
import socket


FIREX_BIN_DIR_ENV = 'firex_bin_dir'


def delimit2list(str_to_split, delimiters=(',', ';', '|', ' ')) -> []:
    if not str_to_split:
        return []

    if not isinstance(str_to_split, str):
        return str_to_split

    # regex for only comma is (([^,'"]|"(?:\\.|[^"])*"|'(?:\\.|[^'])*')+)
    regex = """(([^""" + "".join(delimiters).replace(" ", "\s") + """'"]|"(?:\\.|[^"])*"|'(?:\\.|[^'])*')+)"""
    tokens = re.findall(regex, str_to_split)

    # unquote "tokens" if necessary
    tokens = [g1 if g1.strip() != g2.strip() or g2[0] not in "'\"" else g2.strip(g2[0]) for g1, g2 in tokens]
    tokens = [t.strip() for t in tokens]  # remove extra whitespaces
    tokens = [t.strip("".join(delimiters) + " ") for t in tokens]  # remove any extra (or lone) delimiters
    tokens = [t for t in tokens if t]  # remove empty tokens
    return tokens


def get_available_port(so_reuseport=True):
    sock = socket.socket()
    if so_reuseport and hasattr(socket, 'SO_REUSEPORT'):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    return port


def silent_mkdir(path, mode=0o777, exist_ok=True):
    os.makedirs(path, mode=mode, exist_ok=exist_ok)


def poll_until_file_exist(file_path, timeout=10):
    timeout_time = time.time() + timeout
    while not os.path.exists(file_path) and time.time() < timeout_time:
        time.sleep(0.1)
    assert os.path.isfile(file_path), 'File %s did not exist within %r seconds' % (file_path, timeout)


def poll_until_existing_file_not_empty(file_path, timeout=10):
    timeout_time = time.time() + timeout
    while os.stat(file_path).st_size == 0 and time.time() < timeout_time:
        time.sleep(0.1)
    assert os.stat(file_path).st_size > 0, 'File %s size is zero' % file_path


def poll_until_file_not_empty(file_path, timeout=10):
    start_time = time.time()
    poll_until_file_exist(file_path, timeout)
    remaining_timeout = timeout - (time.time() - start_time)
    poll_until_existing_file_not_empty(file_path, remaining_timeout)


def poll_until_dir_empty(dir_path, timeout=15):
    timeout_time = time.time() + timeout
    while len(os.listdir(dir_path)) > 0 and time.time() < timeout_time:
        time.sleep(0.1)
    return not os.listdir(dir_path)


def proc_matches(proc_info, pname, cmdline_regex, cmdline_contains):
    if proc_info['name'] == pname:
        if cmdline_regex:
            return any(cmdline_regex.search(item) for item in proc_info['cmdline'])
        elif cmdline_contains:
            return any(cmdline_contains in item for item in proc_info['cmdline'])
        else:
            return True
    else:
        return False


def find_procs(name, cmdline_regex=None, cmdline_contains=None):
    matching_procs = []
    if cmdline_regex:
        cmdline_regex = re.compile(cmdline_regex)
    else:
        cmdline_regex = None
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['name', 'cmdline', 'pid'])
        except psutil.NoSuchProcess:
            pass
        else:
            if proc_matches(pinfo, name, cmdline_regex, cmdline_contains):
                matching_procs.append(proc)

    return matching_procs


def wait_until(predicate, timeout, sleep_for, *args, **kwargs):
    max_time = time.time() + timeout
    while time.time() < max_time:
        if predicate(*args, **kwargs):
            return True
        time.sleep(sleep_for)
    return predicate(*args, **kwargs)


def wait_until_pid_not_exist(pid, timeout=7, sleep_for=1):
    return wait_until(lambda p: not psutil.pid_exists(p), timeout, sleep_for, pid)


def qualify_firex_bin(bin_name):
    if FIREX_BIN_DIR_ENV in os.environ:
        return os.path.join(os.environ[FIREX_BIN_DIR_ENV], bin_name)
    return bin_name


def select_env_vars(env_names):
    return {k: v for k, v in os.environ.items() if k in env_names}
