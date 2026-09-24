"""
The check_output/check_call/run wrappers around subprocess.
"""

import os
import subprocess
from pathlib import Path

import pytest

from firexapp import firex_subprocess
from firexapp.firex_subprocess import CommandFailed, ProcStats
from firexkit.firex_exceptions import FireXInactivityTimeoutExpired

TEST_TEXT = "This is a good test"
ECHO_TEST_TEXT = f"/bin/echo {TEST_TEXT}"


@pytest.fixture
def output_file(tmp_path) -> Path:
    """A path for a runner's output file, which the runner is expected to create.

    Deliberately per-test: copy_file_path is a no-op when the destination
    already exists, so a path shared with another test is a path that silently
    stops being written.
    """
    return tmp_path / "runner_output.txt"


def _first_line(path: Path) -> str:
    return path.read_text().splitlines()[0].strip()


def _append_to_file_cmd(path: Path, iterations: int = 2) -> str:
    """A command that appends to 'path' once a second, writing nothing to stdout."""
    return (
        f'bash -c "for v in {{1..{iterations}}};'
        f"do echo '{TEST_TEXT}' >> {path};sleep 1;done\""
    )


class TestCheckOutput:
    def test_output_is_returned_and_copied(self, output_file):
        returned = firex_subprocess.check_output(
            ECHO_TEST_TEXT, copy_file_path=output_file
        )
        assert returned.strip() == TEST_TEXT
        assert _first_line(output_file) == TEST_TEXT

    def test_a_failing_command_raises_with_stdout(self):
        with pytest.raises(CommandFailed) as exc_info:
            firex_subprocess.check_output("exit 1", shell=True)
        assert exc_info.value.stdout is not None

    def test_a_timeout_raises_with_stdout(self):
        with pytest.raises(subprocess.TimeoutExpired) as exc_info:
            firex_subprocess.check_output("cat", timeout=1)
        assert exc_info.value.stdout is not None

    def test_an_inactivity_timeout_raises_with_stdout(self):
        with pytest.raises(FireXInactivityTimeoutExpired) as exc_info:
            firex_subprocess.check_output("cat", inactivity_timeout=1)
        assert exc_info.value.stdout is not None


class TestCheckCall:
    def test_it_returns_nothing_and_writes_no_file(self, output_file):
        assert firex_subprocess.check_call(ECHO_TEST_TEXT) is None
        assert not output_file.exists()

    def test_output_is_written_to_the_given_file(self, output_file):
        assert firex_subprocess.check_call(ECHO_TEST_TEXT, file=output_file) is None
        assert _first_line(output_file) == TEST_TEXT

    def test_a_failing_command_raises_without_stdout(self):
        with pytest.raises(CommandFailed) as exc_info:
            firex_subprocess.check_call("exit 1", shell=True)
        assert exc_info.value.stdout is None

    def test_an_inactivity_timeout_raises_without_stdout(self):
        with pytest.raises(FireXInactivityTimeoutExpired) as exc_info:
            firex_subprocess.check_call("cat", inactivity_timeout=1)
        assert exc_info.value.stdout is None


class TestRun:
    def test_it_returns_a_completed_process(self, output_file):
        result = firex_subprocess.run(ECHO_TEST_TEXT)
        assert isinstance(result, subprocess.CompletedProcess)
        assert result.stdout is not None
        assert not output_file.exists()

    def test_captured_output_is_returned_and_copied(self, output_file):
        result = firex_subprocess.run(
            ECHO_TEST_TEXT,
            capture_output=True,
            copy_file_path=output_file,
        )
        assert isinstance(result, subprocess.CompletedProcess)
        assert result.stdout.strip() == TEST_TEXT
        assert _first_line(output_file) == TEST_TEXT

    def test_a_failing_command_does_not_raise_by_default(self):
        result = firex_subprocess.run("exit 1", shell=True, capture_output=True)
        assert result.stderr is not None

    def test_check_raises_on_a_failing_command(self):
        with pytest.raises(CommandFailed) as exc_info:
            firex_subprocess.run("exit 1", shell=True, capture_output=True, check=True)
        assert exc_info.value.stdout is not None

    def test_an_inactivity_timeout_without_capture_raises_without_stdout(self):
        with pytest.raises(FireXInactivityTimeoutExpired) as exc_info:
            firex_subprocess.run("cat", inactivity_timeout=1, capture_output=False)
        assert exc_info.value.stdout is None


class TestFirexPythonPathRemoval:
    """firex's own PYTHONPATH is kept out of the command's environment."""

    @staticmethod
    def _echoed_pythonpath(**kwargs) -> str:
        return firex_subprocess.check_output(
            "/bin/echo $PYTHONPATH", shell=True, **kwargs
        ).strip()

    def test_the_firex_pythonpath_is_not_passed_on(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", "firex_path")
        assert self._echoed_pythonpath() == ""

    def test_removal_can_be_turned_off(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", "firex_path")
        echoed = self._echoed_pythonpath(
            remove_firex_pythonpath=False,
            env={"PYTHONPATH": "some value"},
        )
        assert echoed == "some value"

    @pytest.mark.parametrize(
        "user_paths",
        ["some_path", "some_path:some_path2"],
        ids=["one_path", "several_paths"],
    )
    def test_only_the_users_own_paths_survive(self, monkeypatch, user_paths):
        monkeypatch.setenv("PYTHONPATH", "start_path")
        echoed = self._echoed_pythonpath(env={"PYTHONPATH": f"start_path:{user_paths}"})
        assert echoed == user_paths

    def test_an_env_without_pythonpath_is_used_as_is(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", "start_path")
        env = os.environ.copy()
        env.pop("PYTHONPATH")
        assert self._echoed_pythonpath(env=env) == ""

    def test_an_empty_pythonpath_stays_empty(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", "start_path")
        env = os.environ.copy() | {"PYTHONPATH": ""}
        assert self._echoed_pythonpath(env=env) == ""


class TestActivityMonitoring:
    """A command silent on stdout is still alive if it writes a monitored file."""

    def test_writing_a_monitored_file_staves_off_the_inactivity_timeout(self, tmp_path):
        monitored_file = tmp_path / "monitored.txt"
        monitored_file.touch()

        returned = firex_subprocess.check_output(
            _append_to_file_cmd(monitored_file),
            inactivity_timeout=1,
            monitor_activity_files=["./*"],
            cwd=str(tmp_path),
        )

        assert returned == ""
        assert _first_line(monitored_file) == TEST_TEXT

    def test_writing_an_unmonitored_file_does_not(self, tmp_path):
        unmonitored_file = tmp_path / "unmonitored.txt"
        unmonitored_file.touch()

        with pytest.raises(FireXInactivityTimeoutExpired) as exc_info:
            firex_subprocess.check_output(
                _append_to_file_cmd(unmonitored_file),
                inactivity_timeout=1,
            )

        assert exc_info.value.stdout is not None


class TestProcStats:
    # Caution: very long-running test!
    def test_stats(self):
        stats = ProcStats()
        num_cpu = os.cpu_count()

        cmd = "seq {num_procs:d} | xargs -P0 -n1 timeout {timeout} md5sum /dev/zero"

        # Test with 50% of cpu
        if num_cpu != 1:
            num_procs = round(num_cpu / 2)
            firex_subprocess.run(
                cmd.format(num_procs=num_procs, timeout=5),
                shell=True,
                proc_stats=stats,
                timeout=10,
            )
            expected = 100 * num_procs / num_cpu
            assert expected - 15 < stats.cpu_percent_used < expected + 15, (
                f"Expected {expected}% CPU but got {stats.cpu_percent_used}"
            )
            assert stats.mem_mb_used != 0.0
            assert stats.mem_mb_high_wm != 0.0

        # 100% CPU
        num_procs = num_cpu
        firex_subprocess.run(
            cmd.format(num_procs=num_procs, timeout=5),
            shell=True,
            proc_stats=stats,
            timeout=10,
        )
        expected = 100
        assert expected - 25 < stats.cpu_percent_used < expected + 25, (
            f"Expected {expected}% CPU but got {stats.cpu_percent_used}"
        )
        assert stats.mem_mb_used != 0
        assert stats.mem_mb_high_wm != 0

        # 200% CPU
        # deliberately in the same test: the memory expectations below are
        # relative to what the 100% CPU run above measured.
        mem_expected = stats.mem_mb_used * 2
        mem_hw_expected = stats.mem_mb_high_wm * 2
        num_procs = num_cpu * 2
        # Need double time to let procs start / finnish
        firex_subprocess.run(
            cmd.format(num_procs=num_procs, timeout=10),
            shell=True,
            proc_stats=stats,
            timeout=20,
        )
        expected = 100  # CPU cannot go above 100%
        assert expected - 25 < stats.cpu_percent_used < expected + 25, (
            f"Expected {expected}% CPU but got {stats.cpu_percent_used}"
        )
        # Loose check due to experience
        assert mem_expected * 0.50 <= stats.mem_mb_used < mem_expected * 1.30, (
            f"Expected {mem_expected} MB memory but got {stats.mem_mb_used}"
        )
        assert (
            mem_hw_expected * 0.50 <= stats.mem_mb_high_wm < mem_hw_expected * 1.30
        ), f"Expected {mem_hw_expected} MB max memory but got {stats.mem_mb_high_wm}"

        # zero running time
        firex_subprocess.run("/bin/echo hello", proc_stats=stats, timeout=10)
        assert stats.elapsed_time == 0
        assert stats.cpu_percent_used == 0
        assert stats.mem_mb_used == 0
        assert stats.mem_mb_high_wm == 0
        assert stats.num_cpu != 0
