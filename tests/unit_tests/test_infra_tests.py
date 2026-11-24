import contextlib
import datetime
import fnmatch
import glob
import inspect
import io
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import coverage
import pytest
from lxml import etree
from xmlrunner.runner import XMLTestRunner

from firexapp import subprocess_coverage
from firexapp.subprocess_coverage import (
    COV_DATAFILE_ENV,
    COVERAGE_ENV_VARS,
    coverage_env,
    coverage_env_cmd_prefix,
    coverage_env_from_os_environ,
)
from firexapp.testing import test_infra
from firexapp.testing.config_interpreter import (
    ConfigInterpreter,
    get_packaged_coveragerc_path,
)
from firexapp.testing.test_infra import _set_xunit_suite_start

SUITE_START = datetime.datetime(
    2026, 10, 23, 9, 31, tzinfo=datetime.timezone(datetime.timedelta(hours=-7))
)


def _example_flow_test_case() -> type[unittest.TestCase]:
    # Built on demand rather than at module level, so it isn't collected as a test
    # of its own.
    def test_prints_markup(self):
        print("output with <markup> & ]]> in it")

    return type(
        "ExampleFlowTest",
        (unittest.TestCase,),
        {"test_prints_markup": test_prints_markup},
    )


class SetXunitSuiteStartTests(unittest.TestCase):
    def setUp(self):
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        self.tmp_dir = tmp_dir.name

    def _run_xmlrunner(self) -> str:
        runner = XMLTestRunner(
            output=self.tmp_dir, outsuffix="results", stream=io.StringIO()
        )
        # xmlrunner echoes the output it captures to sys.stdout too.
        with contextlib.redirect_stdout(io.StringIO()):
            runner.run(
                unittest.defaultTestLoader.loadTestsFromTestCase(
                    _example_flow_test_case()
                )
            )
        [xunit_path] = glob.glob(os.path.join(self.tmp_dir, "*.xml"))
        return xunit_path

    def test_replaces_xmlrunners_naive_stop_time_with_the_suite_start(self):
        xunit_path = self._run_xmlrunner()
        testsuite = etree.parse(xunit_path).getroot()
        # what's being fixed: the last testcase's stop time, without an offset.
        self.assertIsNone(
            datetime.datetime.fromisoformat(testsuite.get("timestamp")).tzinfo
        )

        _set_xunit_suite_start(xunit_path, SUITE_START)

        rewritten = etree.parse(xunit_path).getroot()
        self.assertEqual("2026-10-23T09:31:00-07:00", rewritten.get("timestamp"))
        self.assertEqual(testsuite.get("time"), rewritten.get("time"))

    def test_preserves_the_rest_of_the_xunit(self):
        xunit_path = self._run_xmlrunner()
        system_out = etree.parse(xunit_path).findtext(".//system-out")
        self.assertIn("<markup> & ]]>", system_out)

        _set_xunit_suite_start(xunit_path, SUITE_START)

        self.assertEqual(system_out, etree.parse(xunit_path).findtext(".//system-out"))
        with open(xunit_path) as f:
            self.assertIn("<![CDATA[", f.read())

    def test_sets_every_testsuite_under_a_testsuites_root(self):
        xunit_path = os.path.join(self.tmp_dir, "xunit.xml")
        with open(xunit_path, "w") as f:
            f.write(
                "<testsuites>"
                '<testsuite name="a" timestamp="2026-10-23T09:41:00"/>'
                '<testsuite name="b" timestamp="2026-10-23T09:42:00"/>'
                "</testsuites>"
            )

        _set_xunit_suite_start(xunit_path, SUITE_START)

        self.assertEqual(
            ["2026-10-23T09:31:00-07:00"] * 2,
            [
                ts.get("timestamp")
                for ts in etree.parse(xunit_path).getroot().iter("testsuite")
            ],
        )


# Coverage for the processes a run fans out into.
#
# A worker that stops being measured doesn't fail anything -- it just quietly
# stops contributing data -- so what is worth pinning down here is the
# contract that decides whether a process gets measured at all.


class _UtFlowTestConfig:
    """A test config that is measured, i.e. one that didn't opt out."""


class _UtNoCoverageFlowTestConfig:
    no_coverage = True


def _ut_coverage_env(tmp_path) -> dict[str, str]:
    return coverage_env(
        data_file=str(tmp_path / "coverage_data" / ".coverage"),
        config_file=get_packaged_coveragerc_path(),
    )


def test_an_ordinary_subprocess_is_measured(tmp_path):
    """The whole design rests on this: a process is measured because it
    inherited an environment, not because anything wrapped or re-exec'd it.

    That contract lives in pytest-cov's .pth file, which is site-packages
    state rather than something this repo can assert on by reading itself. So
    start a real child the plain way a celery worker is started, and look for
    what it leaves behind.
    """
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()

    subprocess.run(
        [sys.executable, "-c", "import firexapp.subprocess_coverage"],
        env=os.environ | _ut_coverage_env(tmp_path),
        check=True,
    )

    data_files = list(data_dir.glob(".coverage.*"))
    assert data_files, (
        "the child wrote no coverage data. pytest-cov's .pth is what starts"
        " coverage at interpreter startup, so check pytest-cov is installed"
    )
    # Suffixed per process, which is what lets every process in a run -- on
    # this host and on others -- write into one directory on NFS.
    hosts = {
        match.group("host")
        for f in data_files
        if (match := subprocess_coverage._COVERAGE_DATA_FILE_RE.match(f.name))
    }
    assert hosts == {socket.gethostname()}, (
        f"test_infra's per-host report won't recognise {[f.name for f in data_files]}"
    )


def test_coverage_env_reaches_a_run_that_is_being_measured(tmp_path):
    interpreter = ConfigInterpreter()
    interpreter.coverage_data_file = str(tmp_path / ".coverage")

    run_env = interpreter.get_run_env(_UtFlowTestConfig())

    assert set(COVERAGE_ENV_VARS) <= run_env.keys()
    assert run_env[COV_DATAFILE_ENV] == str(tmp_path / ".coverage")


def test_a_no_coverage_config_is_not_measured(tmp_path):
    """Opting out has to mean the environment is never built.

    A config sets no_coverage because tracing changes what it tests, so it
    isn't enough for its own process to skip coverage: the workers it starts
    inherit this environment, and would otherwise be measured anyway.
    """
    interpreter = ConfigInterpreter()
    interpreter.coverage_data_file = str(tmp_path / ".coverage")

    run_env = interpreter.get_run_env(_UtNoCoverageFlowTestConfig())

    assert not set(COVERAGE_ENV_VARS) & run_env.keys()


def test_no_coverage_env_when_the_suite_was_not_run_with_coverage():
    assert not ConfigInterpreter().get_run_env(_UtFlowTestConfig())


def test_the_cmd_prefix_carries_coverage_over_a_hop_that_drops_the_environment(
    tmp_path, monkeypatch
):
    """ssh hands a command the environment sshd chooses, so the variables go
    in the command itself rather than alongside it."""
    expected_env = _ut_coverage_env(tmp_path)
    for name, value in expected_env.items():
        monkeypatch.setenv(name, value)

    prefix = coverage_env_cmd_prefix()

    assert prefix[0] == "env"
    assert set(prefix[1:]) == {f"{n}={v}" for n, v in expected_env.items()}


def test_nothing_is_propagated_by_a_process_that_is_not_measured(monkeypatch):
    for name in COVERAGE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    assert coverage_env_from_os_environ() == {}
    assert coverage_env_cmd_prefix() == []


def _ut_write_coverage_data(data_dir, suffix: str, lines: dict[str, list[int]]) -> None:
    """A data file shaped exactly like one a measured process leaves behind."""
    data = coverage.CoverageData(basename=str(data_dir / ".coverage"), suffix=suffix)
    data.add_lines(lines)
    data.write()


def _ut_combined_lines(data_dir) -> set[tuple[str, int]]:
    data = coverage.CoverageData(basename=str(data_dir / ".coverage"))
    data.read()
    return {
        (measured, line)
        for measured in data.measured_files()
        for line in data.lines(measured) or []
    }


def test_data_written_after_the_last_test_still_reaches_the_report(
    tmp_path, monkeypatch
):
    """Workers outlive the suite, and combine deletes what it reads.

    So a data file that lands after the directory has been listed isn't just
    late, it is dropped -- and the processes that finish last are the pool
    children that ran the tasks. Combining has to wait them out.

    The late write lands well inside the quiet period, which is what the wait
    can actually promise: a process that goes quiet for longer than that is
    indistinguishable from one that has exited.
    """
    quiet_secs = 2
    monkeypatch.setattr(subprocess_coverage, "_COVERAGE_SETTLE_QUIET_SECS", quiet_secs)
    monkeypatch.setattr(subprocess_coverage, "_COVERAGE_SETTLE_TIMEOUT_SECS", 30)
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()
    _ut_write_coverage_data(data_dir, "uthost.100.1", {"/ut/submit.py": [1, 2]})

    def _write_late():
        time.sleep(quiet_secs / 4)
        _ut_write_coverage_data(data_dir, "uthost.200.2", {"/ut/worker.py": [7]})

    late_writer = threading.Thread(target=_write_late)
    late_writer.start()
    try:
        subprocess_coverage.combine_coverage_data(str(data_dir))
    finally:
        late_writer.join()

    assert _ut_combined_lines(data_dir) == {
        ("/ut/submit.py", 1),
        ("/ut/submit.py", 2),
        ("/ut/worker.py", 7),
    }
    assert not list(data_dir.glob(".coverage.*")), "combine left data behind"


def test_combining_an_empty_run_says_so_rather_than_failing(
    tmp_path, monkeypatch, capsys
):
    """A run where nothing was measured is a wiring problem, not a crash."""
    monkeypatch.setattr(subprocess_coverage, "_COVERAGE_SETTLE_QUIET_SECS", 0)
    monkeypatch.setattr(subprocess_coverage, "_COVERAGE_SETTLE_TIMEOUT_SECS", 2)
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()

    subprocess_coverage.combine_coverage_data(str(data_dir))

    assert "No coverage data was collected" in capsys.readouterr().err


def _ut_settle_immediately(monkeypatch) -> None:
    monkeypatch.setattr(subprocess_coverage, "_COVERAGE_SETTLE_QUIET_SECS", 0)
    monkeypatch.setattr(subprocess_coverage, "_COVERAGE_SETTLE_TIMEOUT_SECS", 2)


def test_combining_uses_this_interpreters_coverage_not_the_one_on_path(
    tmp_path, monkeypatch
):
    """A worker StartMaster starts over ssh has the login shell's PATH, on
    which 'coverage' can be another Python's: /router/bin's is a Python 2.7
    coverage 4.x, which skipped every data file and then failed to report."""
    _ut_settle_immediately(monkeypatch)
    wrong_bin = tmp_path / "bin"
    wrong_bin.mkdir()
    wrong_coverage = wrong_bin / "coverage"
    wrong_coverage.write_text("#!/bin/sh\nexit 1\n")
    wrong_coverage.chmod(0o755)
    monkeypatch.setenv("PATH", f"{wrong_bin}{os.pathsep}{os.environ['PATH']}")
    source = tmp_path / "ut_measured.py"
    source.write_text("a = 1\nb = 2\n")
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()
    _ut_write_coverage_data(data_dir, "uthost.100.1", {str(source): [1, 2]})

    subprocess_coverage.combine_coverage_data(
        str(data_dir), xml_report_file=str(data_dir / "coverage.xml")
    )

    assert _ut_combined_lines(data_dir) == {(str(source), 1), (str(source), 2)}
    assert (data_dir / "coverage.xml").is_file()


def test_combining_can_keep_every_processes_own_data(tmp_path, monkeypatch, capsys):
    _ut_settle_immediately(monkeypatch)
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()
    _ut_write_coverage_data(data_dir, "uthost.100.1", {"/ut/submit.py": [1]})
    _ut_write_coverage_data(data_dir, "uthost.200.2", {"/ut/worker.py": [7]})

    subprocess_coverage.combine_coverage_data(str(data_dir), keep_data_files=True)

    assert _ut_combined_lines(data_dir) == {("/ut/submit.py", 1), ("/ut/worker.py", 7)}
    assert sorted(f.name for f in data_dir.glob(".coverage.*")) == [
        ".coverage.uthost.100.1",
        ".coverage.uthost.200.2",
    ]
    assert "arrived late" not in capsys.readouterr().out, (
        "files kept on purpose were taken for stragglers"
    )


def test_a_failed_combine_says_why(tmp_path, monkeypatch):
    """The report is all there is to go on afterwards: a bare exit status left
    the PATH problem above to be reproduced by hand."""
    _ut_settle_immediately(monkeypatch)
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()
    (data_dir / ".coverage.uthost.100.1").write_text("not a coverage data file")

    with pytest.raises(RuntimeError, match="No usable data files"):
        subprocess_coverage.combine_coverage_data(str(data_dir))


def test_the_xml_report_is_relative_to_the_workspace_ctc_reads_it_in(
    tmp_path, monkeypatch
):
    """The cflow chain's CTC++ genctcreport runs in the workspace, and its
    xml2lang_json.py reads Python coverage only from XML whose <source> is
    that workspace -- the shape pytest-cov's '--cov <ws>' gives the 'pytest'
    framework. An empty <source> crashed it into an empty report."""
    _ut_settle_immediately(monkeypatch)
    ws = tmp_path / "ws"
    in_ws = ws / "src" / "pkg" / "mod.py"
    outside_ws = tmp_path / "elsewhere" / "other.py"
    for source in (in_ws, outside_ws):
        source.parent.mkdir(parents=True)
        source.write_text("a = 1\nb = 2\n")
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()
    _ut_write_coverage_data(
        data_dir, "uthost.100.1", {str(in_ws): [1, 2], str(outside_ws): [1]}
    )
    xml_report = data_dir / "coverage.xml"

    subprocess_coverage.combine_coverage_data(
        str(data_dir), xml_report_file=str(xml_report), xml_report_root=str(ws)
    )

    report = etree.parse(str(xml_report))
    assert report.xpath("/coverage/sources/source/text()") == [os.path.realpath(ws)]
    assert report.xpath("//class/@filename") == ["src/pkg/mod.py"], (
        "paths have to be relative to the workspace, and only what is in it"
    )


def test_firex_installed_into_site_packages_is_measured():
    """CI's staging venvs install FireX into lib/python3.x/site-packages, so
    an omit pattern like '*/lib/*' measures nothing there."""
    config = coverage.Coverage(config_file=get_packaged_coveragerc_path()).config
    installed = "/staging/venv/lib/python3.10/site-packages/microservices/tasks.py"

    assert not [p for p in config.run_omit if fnmatch.fnmatch(installed, p)]


def test_measuring_subprocesses_measures_what_the_block_starts(tmp_path):
    """How a pyflowtest gets measured: pytest runs in this process, and the
    runs it submits inherit os.environ, so the block's environment is the
    whole mechanism."""
    data_dir = tmp_path / "cflow"

    with subprocess_coverage.measuring_subprocesses(
        str(data_dir), config_file=get_packaged_coveragerc_path()
    ):
        subprocess.run(
            [sys.executable, "-c", "import firexapp.subprocess_coverage"], check=True
        )

    assert list(data_dir.glob(".coverage.*")), "the child inherited nothing"


def test_measuring_subprocesses_restores_the_environment_even_when_raising(tmp_path):
    """It mutates the celery worker's own environment, so the restore has to
    survive a failing test session."""
    before = {name: os.environ.get(name) for name in COVERAGE_ENV_VARS}

    with (
        contextlib.suppress(RuntimeError),
        subprocess_coverage.measuring_subprocesses(
            str(tmp_path / "cflow"), config_file=get_packaged_coveragerc_path()
        ),
    ):
        assert subprocess_coverage.is_measured()
        raise RuntimeError("the test session failed")

    assert {name: os.environ.get(name) for name in COVERAGE_ENV_VARS} == before


def test_a_caller_supplied_data_dir_is_left_for_the_caller_to_combine(tmp_path):
    """run-flowtests is invoked once per test file by RunFireXLegacyFlowtests.

    Combining in each of them would delete the raw data the others still need
    merging with, and Cobertura XML can't be merged afterwards -- so whoever
    owns the directory does the combining.
    """
    shared = str(tmp_path / "shared_cflow")

    settings = test_infra.resolve_coverage_settings(
        coverage=False,
        coverage_data_dir=shared,
        results_directory=str(tmp_path / "results"),
        no_html=False,
    )

    assert settings.enabled, "--coverage_data_dir has to imply --coverage"
    assert settings.data_dir == shared
    assert not settings.combine_here
    assert settings.html_report_dir is None


def test_coverage_without_a_data_dir_combines_and_reports_for_itself(tmp_path):
    results = str(tmp_path / "results")

    settings = test_infra.resolve_coverage_settings(
        coverage=True,
        coverage_data_dir=None,
        results_directory=results,
        no_html=False,
    )

    assert settings.enabled
    assert settings.data_dir == os.path.join(results, test_infra.COVERAGE_DATA_DIR_NAME)
    assert settings.combine_here
    assert settings.html_report_dir == os.path.join(results, "coverage")


def test_no_coverage_measures_nothing(tmp_path):
    settings = test_infra.resolve_coverage_settings(
        coverage=False,
        coverage_data_dir=None,
        results_directory=str(tmp_path),
        no_html=False,
    )

    assert not settings.enabled
    assert not settings.combine_here


def test_an_older_subclass_that_branches_on_coverage_still_works():
    """A firexapp release has to be consumable before the repos subclassing
    ConfigInterpreter catch up.

    firex_cisco's FireXConfigInterpreter.get_exe branches on self.coverage to
    decide whether to wrap firex in 'coverage run'. Removing the attribute
    broke every legacy flowtest there, not just measured ones, because get_exe
    runs for all of them. It stays, and stays false: wrapping is no longer how
    anything is measured, so the un-wrapped branch is the right one.
    """
    interpreter = ConfigInterpreter()
    interpreter.coverage_data_file = "/ut/coverage_data/.coverage"

    assert interpreter.coverage is False
    assert interpreter.is_coverage_enabled(_UtFlowTestConfig()), (
        "the run is still measured, just not by wrapping the command"
    )


def test_measuring_in_process_creates_coverage_data(tmp_path):
    """measuring_in_process starts a Coverage object in the current process and
    saves coverage data for the code run in the with block."""
    measuring_before = coverage.Coverage.current()
    with subprocess_coverage.measuring_in_process(
        str(tmp_path), config_file=get_packaged_coveragerc_path()
    ):
        # Run some code that will be measured.
        x = 1 + 1
        assert x == 2

    # Coverage data should have been written.
    files = os.listdir(str(tmp_path))
    assert any(f.startswith(".coverage.") for f in files), (
        f"expected coverage data files, got {files}"
    )
    # The pool child goes on to run other tasks, which nothing would save.
    assert coverage.Coverage.current() is measuring_before, "still measuring"


def test_measuring_in_process_restores_on_exception(tmp_path):
    """When an exception is raised in the block, coverage still saves."""
    with (
        contextlib.suppress(RuntimeError),
        subprocess_coverage.measuring_in_process(
            str(tmp_path), config_file=get_packaged_coveragerc_path()
        ),
    ):
        x = 1 + 1
        assert x
        raise RuntimeError("test exception")

    # Coverage data should still have been written, despite the exception.
    files = os.listdir(str(tmp_path))
    assert any(f.startswith(".coverage.") for f in files), (
        f"expected coverage data despite exception, got {files}"
    )


def test_measuring_in_process_leaves_a_process_measuring_there_alone(tmp_path):
    """A pool child of a measured worker already measures into the run's
    directory. A second Coverage would only pause that one for the block."""
    already = coverage.Coverage(
        data_file=str(tmp_path / ".coverage"),
        data_suffix=True,
        config_file=get_packaged_coveragerc_path(),
    )
    already.start()
    try:
        with subprocess_coverage.measuring_in_process(
            str(tmp_path), config_file=get_packaged_coveragerc_path()
        ):
            assert coverage.Coverage.current() is already
    finally:
        already.stop()

    assert not list(tmp_path.glob(".coverage.*")), "the block wrote data of its own"


# A prefork pool child as billiard runs one: forked from a measured worker
# after its imports, starting and saving its own measurement through the
# worker_process_init/shutdown handlers, and leaving through os._exit().
_UT_POOL_CHILD_SCRIPT = """
import os
from firexapp import subprocess_coverage

pid = os.fork()
if pid == 0:
    subprocess_coverage.start_pool_child_coverage()
    subprocess_coverage.coverage_env_from_os_environ()
    subprocess_coverage.save_pool_child_coverage()
    os._exit(0)
os.waitpid(pid, 0)
print(pid)
"""


def _ut_measured_subprocess_coverage_lines(data_file) -> set[int]:
    data = coverage.CoverageData(basename=str(data_file))
    data.read()
    [measured] = [
        f
        for f in data.measured_files()
        if f.endswith(os.path.join("firexapp", "subprocess_coverage.py"))
    ] or [None]
    return set(data.lines(measured) or []) if measured else set()


def test_a_pool_child_writes_only_what_it_ran(tmp_path):
    """The worker's imports and startup are the worker's to write out.

    A pool child that also wrote the copy it inherits across the fork made
    ~85% of a run's coverage data: ~1MB of the same startup per pool child.
    """
    data_dir = tmp_path / "coverage_data"
    data_dir.mkdir()
    source_lines, _ = inspect.findsource(subprocess_coverage)
    module_level_line = 1 + next(
        i for i, line in enumerate(source_lines) if line.startswith("COV_SOURCE_ENV =")
    )
    body, first_line = inspect.getsourcelines(
        subprocess_coverage.coverage_env_from_os_environ
    )
    child_ran_line = first_line + next(
        i for i, line in enumerate(body) if "not in os.environ" in line
    )

    child_pid = subprocess.run(
        [sys.executable, "-c", _UT_POOL_CHILD_SCRIPT],
        env=os.environ | _ut_coverage_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    [child_file] = data_dir.glob(f".coverage.*.{child_pid}.*")
    child_lines = _ut_measured_subprocess_coverage_lines(child_file)
    assert child_ran_line in child_lines, "what the child ran is missing"
    assert module_level_line not in child_lines, "the child wrote the worker's startup"
    assert any(
        module_level_line in _ut_measured_subprocess_coverage_lines(f)
        for f in data_dir.glob(".coverage.*")
        if f != child_file
    ), "nothing wrote the worker's startup"
