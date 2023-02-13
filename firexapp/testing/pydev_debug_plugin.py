import inspect
import json
import os
import sys
from _socket import gethostname

from firexapp.fileregistry import FileRegistry
from firexapp.submit.uid import Uid

PYDEV_REGISTRY_KEY = 'pydev_debug'
FileRegistry().register_file(PYDEV_REGISTRY_KEY,
                             os.path.join(Uid.debug_dirname, 'pydev_debug.json'))


def get_pydev_debug_setup():
    frame = inspect.currentframe()
    all_frames = inspect.getouterframes(frame)
    for i in range(1, len(all_frames)):
        setup_frame = all_frames[-i]
        setup = setup_frame.frame.f_locals.get('setup')
        if setup:
            return setup, setup_frame.filename


def is_debugging():
    frame = inspect.currentframe()
    top_frame = inspect.getouterframes(frame)[-1]
    return "pydevd.py" in top_frame.filename


def is_firex_submit():
    frame = inspect.currentframe()
    while frame.f_back:
        if frame.f_code.co_name == "run_submit" and str(frame.f_code.co_filename).endswith("submit.py"):
            return frame
        frame = frame.f_back
    return None


def get_pydev_command(cmd, setup=None, pydev=None, debug_host=None):
    if not setup:
        setup, pydev = get_pydev_debug_setup()
    pydev_cmd = [sys.executable, pydev]
    client = str(setup['client'])

    if "127.0.0.1" in client and debug_host and debug_host != gethostname():
        #  This is broken on newer pycharm, since it binds pydev debugger only to 127.0.0.1:<port>
        client = debug_host
    proc_str = '--multiprocess' if setup.get('multiprocess') else '--multiproc'

    pydev_cmd += [proc_str, '--client', client, '--port', str(setup['port']), '--file']
    if type(cmd) is str:
        cmd = [cmd]
    pydev_cmd += cmd
    return pydev_cmd


def is_celery():
    frame = inspect.currentframe()
    while frame.f_back:
        if str(frame.f_code.co_filename).endswith("celery/bin/celery.py"):
            return frame
        frame = frame.f_back
    return None


def store_debug_info():
    setup, pydev = get_pydev_debug_setup()
    data = {
        "pydev": pydev,
        "setup": setup,
        "debug_host": gethostname()
    }

    from firexapp.engine.celery import app
    logs_dir = app.backend.get('logs_dir').decode()
    json_path = FileRegistry().get_file(PYDEV_REGISTRY_KEY, logs_dir)

    print("Storing debugging information", file=sys.stdout)
    with open(json_path, 'w') as outfile:
        json.dump(data, outfile)


def restart_celery_in_debug():
    from firexapp.engine.celery import app
    firex_logs_dir = app.backend.get('logs_dir').decode()
    json_path = FileRegistry().get_file(PYDEV_REGISTRY_KEY, firex_logs_dir)

    if not os.path.isfile(json_path):
        print("No debugging information found", file=sys.stdout)
        return

    with open(json_path, "r") as infile:
        data = json.load(infile)

    setup = data['setup']
    pydev = data['pydev']
    debug_host = data['debug_host']

    celery_argsv = sys.argv
    debug_celery = get_pydev_command(celery_argsv, setup=setup, pydev=pydev, debug_host=debug_host)
    print("restarting celery in debug mode", file=sys.stdout)
    os.execl(debug_celery[0], *debug_celery)


if is_debugging():
    firex_frame = is_firex_submit()
    if firex_frame:
        # store the debug info to be used when celery restarts
        store_debug_info()
elif is_celery():
    restart_celery_in_debug()
