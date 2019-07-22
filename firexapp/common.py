import time

import os
import re
import socket

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
