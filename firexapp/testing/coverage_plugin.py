import os
import sys
import inspect


def find_in_stack(file_to_find)->bool:
    frame = inspect.currentframe()
    while frame.f_back:
        if str(frame.f_code.co_filename).endswith(file_to_find):
            return True
        frame = frame.f_back
    return False


def is_running_under_coverage()->bool:
    return find_in_stack("coverage/cmdline.py")


def is_celery()->bool:
    return find_in_stack("celery/bin/celery.py")


def restart_celery_under_coverage():
    print("Restarting celery in coverage...", file=sys.stdout)

    # assemble new command line
    import coverage
    coverage_cmd = [sys.executable, os.path.dirname(coverage.__file__), "run", "--branch", "--parallel-mode", "-m"]
    coverage_celery = coverage_cmd + ["celery"] + sys.argv[1:]
    # coverage does not support forks
    coverage_celery +=["--pool=eventlet"]
    print(coverage_celery)

    # restart the process under coverage
    os.execlp(sys.executable, *coverage_celery)


if is_running_under_coverage():
    print("Currently running under coverage")
elif is_celery():
    restart_celery_under_coverage()
