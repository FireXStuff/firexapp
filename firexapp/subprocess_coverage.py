"""Starting coverage in the subprocesses a FireX run fans out into.

A run is several processes on several hosts: the submit process, the local
'mc' celery worker it starts, and the remote workers StartMaster starts over
ssh. Measuring only the first of those measures almost none of the code under
test.

Activation is by environment variable, never by wrapping a process in
'coverage run' or re-exec'ing it. 'pytest-cov.pth' -- installed into
site-packages by pytest-cov, which is pinned in the authoritative deployment
dependency group and so present in every complete FireX install -- calls
pytest_cov.embed.init() at interpreter startup whenever COV_CORE_SOURCE is
set. A process is therefore measured if and only if it inherited these
variables, which is what lets every worker start through its ordinary code
path, with its ordinary pool, and still be measured.

Nothing here imports coverage or pytest_cov at module scope: this module is
imported on every run, and that import only belongs in the processes actually
being measured.
"""

import contextlib
import os
import re
import subprocess
import sys
import time

# What pytest_cov.embed.init() reads. 'pytest-cov.pth' calls init() when
# COV_CORE_SOURCE is present, so that name is the switch; the others only
# matter once it is set.
COV_SOURCE_ENV = "COV_CORE_SOURCE"
COV_CONFIG_ENV = "COV_CORE_CONFIG"
COV_DATAFILE_ENV = "COV_CORE_DATAFILE"
COV_BRANCH_ENV = "COV_CORE_BRANCH"

COVERAGE_ENV_VARS = (
    COV_SOURCE_ENV,
    COV_CONFIG_ENV,
    COV_DATAFILE_ENV,
    COV_BRANCH_ENV,
)

# The base name every measured process suffixes, and the pattern that reads a
# suffixed name back: '.coverage.<hostname>.<pid>.<random>'. The pid and random
# parts are digits, which is what makes the hostname -- which can contain dots
# itself -- unambiguous to pick out.
COVERAGE_DATA_FILE_BASE_NAME = ".coverage"
_COVERAGE_DATA_FILE_RE = re.compile(r"^\.coverage\.(?P<host>.+)\.\d+\.\d+$")

# How long the coverage data directory has to stop changing before the
# processes still writing into it are taken to be done, and how long to give
# them in total. Generous on both counts: a worker that shuts down slowly costs
# a few seconds here, whereas combining too early costs its data.
_COVERAGE_SETTLE_QUIET_SECS = 5
_COVERAGE_SETTLE_TIMEOUT_SECS = 5 * 60


def coverage_env(
    data_file: str,
    config_file: str,
    branch: bool = True,
) -> dict[str, str]:
    """The environment that switches coverage on for a process and its children.

    'data_file' is a base path rather than a file that gets written:
    embed.init() passes data_suffix=True, so every process appends its own
    '.<hostname>.<pid>.<random>' and none of them share a file. That is what
    makes it safe for the whole fan-out -- including workers on other hosts --
    to point at one directory on NFS, and what leaves 'coverage combine' with
    everything it needs at the end of the run.
    """
    return {
        # Empty rather than unset: the .pth tests for presence, and embed
        # reads an empty value as 'no source restriction' (since '' is in
        # os.pathsep), which leaves what gets measured entirely to
        # 'config_file'.
        COV_SOURCE_ENV: "",
        COV_CONFIG_ENV: config_file,
        COV_DATAFILE_ENV: data_file,
        COV_BRANCH_ENV: "enabled" if branch else "",
    }


def coverage_env_from_os_environ() -> dict[str, str]:
    """This process's coverage environment, to pass on to a child process.

    Empty when this process isn't being measured, so callers can merge it in
    unconditionally rather than testing for coverage themselves.
    """
    if COV_DATAFILE_ENV not in os.environ:
        return {}
    return {name: os.environ[name] for name in COVERAGE_ENV_VARS if name in os.environ}


def coverage_env_cmd_prefix() -> list[str]:
    """An 'env K=V ...' prefix, for a hop that doesn't inherit os.environ.

    A command run over ssh gets the environment sshd gives it. Paramiko's
    'environment=' argument only applies to variables the server's AcceptEnv
    permits and is silently dropped otherwise, so the variables are put in the
    command itself, which no server configuration can discard. Empty when this
    process isn't being measured, so it can be prepended unconditionally.
    """
    env = coverage_env_from_os_environ()
    if not env:
        return []
    return ["env", *(f"{name}={value}" for name, value in env.items())]


def is_measured() -> bool:
    """Whether this process was started with coverage switched on."""
    return COV_DATAFILE_ENV in os.environ


def get_packaged_coveragerc_path() -> str:
    """The config every measured process is pointed at.

    Lives here rather than beside the file in firexapp.testing so that a
    celery worker can reach it: this module deliberately imports nothing, and
    firexapp.testing.config_interpreter pulls in the whole submit harness.
    """
    return os.path.join(os.path.dirname(__file__), "testing", "coveragerc")


@contextlib.contextmanager
def measuring_subprocesses(data_dir: str, config_file: str):
    """Measure everything this process starts, for the duration of the block.

    For callers that run tests in-process -- pytest.main() inside a celery
    task -- rather than by building a command. Everything the block starts
    inherits os.environ and so is measured: the runs a pyflowtest submits, and
    in turn every celery worker those runs start.

    Mutating os.environ is process-wide, which is only acceptable because the
    pytest session being wrapped runs in this process (with '-n0') and so has
    the process to itself. The restore is therefore unconditional.
    """
    added = coverage_env(
        data_file=os.path.join(data_dir, COVERAGE_DATA_FILE_BASE_NAME),
        config_file=config_file,
    )
    os.makedirs(data_dir, exist_ok=True)
    previous = {name: os.environ.get(name) for name in added}
    os.environ.update(added)
    try:
        yield data_dir
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextlib.contextmanager
def measuring_in_process(data_dir: str, config_file: str):
    """Measure this process itself, for the duration of the block.

    Starts a Coverage object in the current process and saves it on exit.
    Used when this process runs tests directly (pytest.main) rather than
    spawning subprocesses to do so. The Coverage object measures application
    code run by pytest.main in this process.

    If pytest-cov's plugin is active (via .pth), it will also measure the
    process. We detect this and log a warning, but proceed anyway -- both
    Coverage objects write to the same data directory (with distinct filenames
    via pid/random suffix), so they merge correctly.

    This is meant to be paired with measuring_subprocesses around any
    subprocesses spawned by the test code, so both the in-process pytest
    execution and any spawned runs/workers are measured.
    """
    # Deferred: only imported when measuring is active.
    from coverage import Coverage

    data_file = os.path.join(data_dir, COVERAGE_DATA_FILE_BASE_NAME)
    cov = Coverage(
        source="",  # Empty: use config_file for source specification
        config_file=config_file,
        branch=True,
        data_file=data_file,
        data_suffix=True,
    )

    # Warn if pytest-cov is already active, but proceed anyway.
    if is_measured():
        print(
            "WARNING: pytest-cov plugin is active (from .pth); measuring with"
            " both the plugin and this Coverage object. Both write to the same"
            " data directory and merge correctly, but this uses extra CPU.",
            file=sys.stderr,
        )

    cov.start()
    try:
        yield data_dir
    finally:
        cov.save()


def coverage_data_files(coverage_data_dir: str) -> list[str]:
    """The per-process data files waiting to be combined, newest name last."""
    return sorted(
        f
        for f in os.listdir(coverage_data_dir)
        if f.startswith(f"{COVERAGE_DATA_FILE_BASE_NAME}.")
    )


def _wait_for_coverage_data_to_settle(coverage_data_dir: str) -> None:
    """Wait for the processes still writing coverage data to finish.

    Workers outlive the tests: celery shuts them down asynchronously and each
    process writes its data on the way out, so the last assertion passing is
    not the last data file appearing. It matters because 'coverage combine'
    deletes what it reads -- a file that lands after it has listed the
    directory isn't merely late, it is dropped from the report, and the
    processes that arrive last are the pool children that ran the tasks.

    There is no handle on those processes from here, so wait for the directory
    to stop changing instead.
    """
    deadline = time.monotonic() + _COVERAGE_SETTLE_TIMEOUT_SECS
    seen = coverage_data_files(coverage_data_dir)
    last_change = time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(1)
        current = coverage_data_files(coverage_data_dir)
        if current != seen:
            seen = current
            last_change = time.monotonic()
        elif time.monotonic() - last_change >= _COVERAGE_SETTLE_QUIET_SECS:
            return
    print(
        f"Coverage data was still arriving after {_COVERAGE_SETTLE_TIMEOUT_SECS}s."
        " Combining anyway; the report may be missing a process or two.",
        file=sys.stderr,
    )


def combine_coverage_data(
    coverage_data_dir: str,
    html_report_dir: str | None = None,
    xml_report_file: str | None = None,
) -> bool:
    """Merge the per-process data files in a directory. False if there were none.

    The per-host counts printed here are the thing to read when a producer
    stops working: a test path that was never measured doesn't fail anything,
    it just quietly stops contributing files, and this is the only place that
    shows.
    """
    _wait_for_coverage_data_to_settle(coverage_data_dir)

    data_files = coverage_data_files(coverage_data_dir)
    if not data_files:
        print(
            f"No coverage data was collected in {coverage_data_dir!r}. Nothing to"
            " combine.",
            file=sys.stderr,
        )
        return False

    files_per_host: dict[str, int] = {}
    for data_file in data_files:
        match = _COVERAGE_DATA_FILE_RE.match(data_file)
        host = match.group("host") if match else "unrecognised"
        files_per_host[host] = files_per_host.get(host, 0) + 1
    hosts = ", ".join(f"{host} ({count})" for host, count in files_per_host.items())
    print(f"Combining {len(data_files)} coverage data files from: {hosts}")

    subprocess.check_output(["coverage", "combine", *data_files], cwd=coverage_data_dir)

    # Combining is itself slow enough for a straggler to land during it, which
    # the settle wait can't rule out. '--append' so this adds to what was just
    # combined rather than starting clean and discarding it.
    stragglers = coverage_data_files(coverage_data_dir)
    if stragglers:
        print(f"Combining {len(stragglers)} further data files that arrived late")
        subprocess.check_output(
            ["coverage", "combine", "--append", *stragglers], cwd=coverage_data_dir
        )

    if xml_report_file:
        subprocess.check_output(
            ["coverage", "xml", "-o", xml_report_file], cwd=coverage_data_dir
        )
        print(f"Coverage XML: {xml_report_file}")
    if html_report_dir:
        print("Generating Coverage Report...")
        subprocess.check_output(
            ["coverage", "html", "-d", html_report_dir], cwd=coverage_data_dir
        )
        print(html_report_dir)
    return True


def start_pool_child_coverage() -> None:
    """Begin measuring a freshly forked celery pool child.

    A child inherits the worker's in-progress measurement across the fork, but
    leaves through os._exit() (billiard.pool.Worker._do_exit), which runs no
    atexit handlers -- so what it inherited would never be written, and
    everything a task actually did in a pool child would be missing from the
    report. Starting again here gives the child a Coverage of its own, under
    its own data suffix, that it can save on the way out.

    This is what lets the worker keep --pool=prefork. The alternative the old
    coverage_plugin took was forcing --pool=eventlet, which measured a
    concurrency model that no production run uses.
    """
    if not is_measured():
        return

    from pytest_cov.embed import init

    init()


def save_pool_child_coverage() -> None:
    """Write out a pool child's measurement before it exits.

    Celery sends worker_process_shutdown from billiard's _do_exit, just before
    the os._exit() that ends the child. That is the only window there is.
    """
    if not is_measured():
        return

    from pytest_cov.embed import cleanup

    cleanup()
