import threading
import time
import os
import psutil
import re
import socket
from threading import get_native_id

from jinja2 import Template
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

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


def get_available_port():
    with socket.socket() as sock:
        sock.bind(('', 0))
        port = sock.getsockname()[1]
    return port


def silent_mkdir(path, exist_ok=True, **kwargs):
    os.makedirs(path, exist_ok=exist_ok, **kwargs)


def poll_until_path_exist(path, timeout=10):
    timeout_time = time.time() + timeout
    path_exists = os.path.exists(path)
    while not path_exists and time.time() < timeout_time:
        time.sleep(0.1)
        path_exists = os.path.exists(path)
    if not path_exists:
        raise AssertionError(f'{path} did not exist within {timeout}s')


def poll_until_file_exist(file_path, timeout=10):
    poll_until_path_exist(file_path, timeout=timeout)
    assert os.path.isfile(file_path), f'{file_path} does not appear to be a file'


def poll_until_existing_file_not_empty(file_path, timeout: float = 10):
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
        cmdline = proc_info['cmdline'] or []
        if cmdline_regex:
            return any(cmdline_regex.search(item) for item in cmdline)
        elif cmdline_contains:
            return any(cmdline_contains in item for item in cmdline)
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

from typing import Callable, TypeVar

T = TypeVar('T')

def wait_until(
    predicate: Callable[..., T],
    timeout: float,
    sleep_for: float,
    *args,
    **kwargs,
) -> T:
    max_time = time.time() + timeout
    while time.time() < max_time:
        r = predicate(*args, **kwargs)
        if r:
            return r
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


def find(keys, input_dict):
    result = input_dict
    for key in keys:
        try:
            result = result[key]
        except (TypeError, KeyError):
            return None
    return result


def render_template(template_str, template_args):
    return Template(template_str).render(**template_args)


#
# Create a symlink to src, named target.
#
# delete_link == True: Delete target link if it exists.
#             == False: Don't delete target link if it exists.
#             == None (default): If symlink creation fails because the link
#                                exists, delete it and try again.
#                                (optimized for cases where we don't expect
#                                the link to exist in most cases.)
#
def create_link(src, target, delete_link=None, relative=False, create_target_dir=False):
    if create_target_dir:
        target_dir = os.path.dirname(target)
        if not os.path.isdir(target_dir):
            silent_mkdir(target_dir)

    if relative:
        src = os.path.relpath(src, os.path.dirname(target))

    if not delete_link:
        try:
            os.symlink(src, target)
            logger.debug(f'Symbolic link created: {target} -> {src}')
            return  # <-- Done!
        except FileExistsError:
            if delete_link is False:
                raise

    # If we want to delete the link, we do a link-replace in an atomic manner
    temp_target = target + f'.{get_native_id()}.tmp'
    try:
        # Avoid errors with possibly stale links
        os.remove(temp_target)
    except FileNotFoundError:
        pass

    try:
        os.symlink(src, temp_target)
        os.rename(temp_target, target)
        logger.debug('Symbolic link created: %s -> %s' % (src, target))
    except Exception:
        try:
            os.remove(temp_target)
        except FileNotFoundError:
            pass

        raise

# Creating link is sometime slow (e.g. on NFS, so do it in a thread
def create_link_async(src: str, target: str, **create_link_kwargs) -> threading.Thread:
    thread = threading.Thread(target=create_link,
                              args=(src, target),
                              kwargs=create_link_kwargs)
    thread.start()
    return thread

def dict2str(mydict, sort=False, sep='    ', usevrepr=True, line_prefix=''):
    if not mydict:
        return 'None'

    txt = ''
    items = mydict.items()
    if sort:
        items = sorted(items)
    maxlen = len(max(mydict.keys(), key=len))
    wrap_space = '\n' + ' ' * (maxlen + len(sep))
    for k, v in items:
        txt += line_prefix
        if usevrepr:
            txt += '%s%s%r\n' % (k.ljust(maxlen, " "), sep, v)
        else:
            v = str(v)
            v = v.replace('\n', wrap_space)
            txt += '%s%s%s\n' % (k.ljust(maxlen, " "), sep, v)
    return txt