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
import shutil
import subprocess
import sys
import tempfile
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
# suffixed name back: '.coverage.<hostname>.<pid>.<random>'. The pid is
# numeric (optionally prefixed with 'pid' in newer coverage releases),
# while newer random suffixes are alphanumeric and may have a content hash.
# Match old numeric names too, including hostnames containing dots.
COVERAGE_DATA_FILE_BASE_NAME = ".coverage"
_COVERAGE_DATA_FILE_RE = re.compile(
    r"^\.coverage\.(?P<host>.+)\.(?:pid)?(?P<pid>\d+)\.\w+(?:\.H\w+)?$"
)

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


def _new_coverage(data_file: str, config_file: str):
    """A Coverage that writes its own suffixed file beside 'data_file'.

    The same measurement pytest_cov.embed.init() starts from the coverage
    variables, for the places that start measuring part way through a process
    rather than at interpreter startup.
    """
    # Deferred: only imported when measuring is active.
    from coverage import Coverage

    return Coverage(
        config_file=config_file,
        branch=True,
        data_file=data_file,
        data_suffix=True,
    )


@contextlib.contextmanager
def measuring_in_process(data_dir: str, config_file: str):
    """Measure this process itself, for the duration of the block.

    For a process that runs tests itself -- pytest.main() in a celery pool
    child -- rather than only through the processes it starts, which is what
    measuring_subprocesses covers. Pair the two.

    A process that is already measuring into this directory -- a pool child
    of a measured worker, say -- covers the block as it is, so nothing more is
    started. Measurement going anywhere else is paused for the block, since
    the block's data belongs in this directory's report.
    """
    # Deferred: only imported when measuring is active.
    from coverage import Coverage

    data_file = os.path.join(data_dir, COVERAGE_DATA_FILE_BASE_NAME)
    current = Coverage.current()
    if current is not None and os.path.abspath(
        current.config.data_file
    ) == os.path.abspath(data_file):
        yield data_dir
        return

    cov = _new_coverage(data_file, config_file)
    cov.start()
    try:
        yield data_dir
    finally:
        # Stopped, not just saved: this process goes on to run other tasks,
        # which nothing would save, and whatever was paused for the block
        # resumes.
        cov.stop()
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


def _run_coverage(*args: str, cwd: str, data_file: str | None = None) -> None:
    """Run coverage's command line as this interpreter's coverage package.

    Never whatever 'coverage' is first on PATH. A worker that StartMaster
    starts over ssh has the login shell's PATH, on which that can be an
    unrelated install: /router/bin/coverage is a Python 2.7 coverage 4.x. It
    can't read these data files, so it skips every one, writes an empty
    combined file, and then fails with 'No data to report'. Isolated (-I) so
    that running in a workspace can't shadow the coverage package either.

    The coverage variables are dropped so that, when this process is itself
    measured, the tool doesn't add data files of its own to the directory it
    is combining. 'data_file' is for running anywhere but beside the data.
    """
    env = {k: v for k, v in os.environ.items() if k not in COVERAGE_ENV_VARS}
    if data_file:
        env["COVERAGE_FILE"] = data_file
    result = subprocess.run(
        [sys.executable, "-I", "-m", "coverage", *args],
        check=False,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    # Printed either way: on success it is the warnings, e.g. a data file that
    # couldn't be read and was left out.
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode:
        raise RuntimeError(
            f"'coverage {args[0]}' failed with exit status {result.returncode}"
            f" in {cwd!r}:\n{result.stdout}"
        )


def combine_coverage_data(
    coverage_data_dir: str,
    html_report_dir: str | None = None,
    xml_report_file: str | None = None,
    keep_data_files: bool = False,
    xml_report_root: str | None = None,
) -> bool:
    """Merge the per-process data files in a directory. False if there were none.

    What is kept is the merged '.coverage' and the reports: combining deletes
    each per-process file once it has been merged in, unless keep_data_files
    -- for when what an individual process measured is the question.

    xml_report_root makes the XML report's paths relative to that directory,
    and leaves out what is outside it. The cflow chain's CTC++ tooling needs
    this to read Python coverage: genctcreport runs in the workspace, and its
    lang_integration_tools/xml2lang_json.py only takes XML whose <source> is
    that workspace, as pytest-cov's '--cov <ws>' writes it for the 'pytest'
    framework. Written anywhere else the report gets an empty <source>, which
    crashes xml2lang_json.py into an empty CTC++ report.

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

    # coverage's data files are SQLite databases, which cost a network round
    # trip per query to read over NFS. From a host far from the file server
    # that dominates: from RTP, against a logs dir in OTT, combining took ~5s a
    # data file and the XML report 17-23 minutes, where both take seconds
    # locally. So it all works on local copies, and only the combined data
    # file and the reports are written back.
    with tempfile.TemporaryDirectory(prefix="firex-coverage-") as local_dir:
        _combine_local_copies(data_files, coverage_data_dir, local_dir, keep_data_files)

        # Combining is itself slow enough for a straggler to land during it,
        # which the settle wait can't rule out. Appended, so this adds to what
        # was just combined rather than starting clean and discarding it. Only
        # names not listed before count: what was kept, or couldn't be read, is
        # still there.
        stragglers = sorted(
            set(coverage_data_files(coverage_data_dir)).difference(data_files)
        )
        if stragglers:
            print(f"Combining {len(stragglers)} further data files that arrived late")
            _combine_local_copies(
                stragglers, coverage_data_dir, local_dir, keep_data_files, append=True
            )

        combined = os.path.join(local_dir, COVERAGE_DATA_FILE_BASE_NAME)
        shutil.copyfile(
            combined, os.path.join(coverage_data_dir, COVERAGE_DATA_FILE_BASE_NAME)
        )

        if xml_report_file:
            if xml_report_root:
                # Canonical, as the measured file names are.
                root = os.path.realpath(xml_report_root)
                _run_coverage(
                    "xml",
                    # Not whatever .coveragerc, setup.cfg etc. the root has.
                    f"--rcfile={get_packaged_coveragerc_path()}",
                    f"--include={root}/*",
                    "-o",
                    os.path.abspath(xml_report_file),
                    cwd=root,
                    data_file=combined,
                )
            else:
                _run_coverage(
                    "xml",
                    "-o",
                    xml_report_file,
                    cwd=coverage_data_dir,
                    data_file=combined,
                )
            print(f"Coverage XML: {xml_report_file}")
        if html_report_dir:
            print("Generating Coverage Report...")
            _run_coverage(
                "html", "-d", html_report_dir, cwd=coverage_data_dir, data_file=combined
            )
            print(html_report_dir)
    return True


def _combine_local_copies(
    names: list[str],
    coverage_data_dir: str,
    local_dir: str,
    keep_data_files: bool,
    append: bool = False,
) -> None:
    """'coverage combine' copies of the named data files into local_dir's '.coverage'.

    The originals then go the way combining them in place would have taken
    them: each one merged in is deleted, unless keep_data_files. One combine
    couldn't read stays, and so does its copy, which is what tells the two
    apart.
    """
    for name in names:
        shutil.copyfile(
            os.path.join(coverage_data_dir, name), os.path.join(local_dir, name)
        )
    _run_coverage("combine", *(["--append"] if append else []), *names, cwd=local_dir)
    if keep_data_files:
        return
    for name in names:
        if not os.path.exists(os.path.join(local_dir, name)):
            os.remove(os.path.join(coverage_data_dir, name))


# The Coverage a pool child measures itself with; see start_pool_child_coverage.
_pool_child_coverage = None


def start_pool_child_coverage() -> None:
    """Begin measuring a freshly forked celery pool child.

    The measurement a child inherits across the fork is the worker's: its
    imports and startup, which the worker writes out itself when it exits.
    The child leaves through os._exit() (billiard.pool.Worker._do_exit), which
    runs no atexit handlers, so to get what its tasks run into the report it
    needs a Coverage of its own that it saves on the way out.

    That Coverage starts empty. pytest_cov.embed.init() would instead first
    save the inherited measurement under the child's pid: a ~1MB copy of the
    worker's startup per pool child, which was ~85% of a run's coverage data.
    Starting this one pauses the inherited one, so the child isn't traced
    twice.

    This is what lets the worker keep --pool=prefork. The alternative the old
    coverage_plugin took was forcing --pool=eventlet, which measured a
    concurrency model that no production run uses.
    """
    global _pool_child_coverage
    if not is_measured():
        return

    _pool_child_coverage = _new_coverage(
        os.environ[COV_DATAFILE_ENV],
        os.environ[COV_CONFIG_ENV],
    )
    _pool_child_coverage.start()


def save_pool_child_coverage() -> None:
    """Write out a pool child's measurement before it exits.

    Celery sends worker_process_shutdown from billiard's _do_exit, just before
    the os._exit() that ends the child. That is the only window there is.
    """
    global _pool_child_coverage
    if _pool_child_coverage is None:
        return

    _pool_child_coverage.stop()
    _pool_child_coverage.save()
    _pool_child_coverage = None
