"""kill_procs_by_name_and_cmdline and firex_procs' finding of FireX processes, against real
processes.

The defunct ones matter most to firex_procs: what a defunct proc does and doesn't still
report is the whole premise of how it finds them, so psutil is never faked here.
"""

import dataclasses
import signal
import subprocess
import sys
import time
import uuid
from getpass import getuser

import psutil
import pytest

from firexkit import firex_procs
from firexkit.firex_procs import (
    FireXProc,
    FireXProcConventions,
    FireXProcQuery,
    FireXProcType,
    RemoteFireXProcException,
    _OsProcInfo,
    exe_base_of,
    force_kill_firex_procs,
)
from firexkit.proc_utils import kill_procs_by_name_and_cmdline


@pytest.fixture
def python(tmp_path) -> str:
    """An interpreter whose processes are named 'python', whatever this one is called."""
    exe = tmp_path / "python"
    exe.symlink_to(sys.executable)
    return str(exe)


def _zombie(python: str) -> subprocess.Popen:
    proc = subprocess.Popen([python, "-c", "pass"])
    # it's a zombie once it has exited, until it's waited on.
    deadline = time.monotonic() + 10
    while psutil.Process(proc.pid).status() != psutil.STATUS_ZOMBIE:
        assert time.monotonic() < deadline, "it never exited"
        time.sleep(0.01)
    return proc


def test_a_zombie_by_the_same_name_doesnt_stop_the_search(python):
    # Matches nothing else that might be running here.
    marker = f"fx-proc-utils-test-{uuid.uuid4()}"
    zombie = _zombie(python)
    target = subprocess.Popen([python, "-c", "import time; time.sleep(60)", marker])
    try:
        killed = kill_procs_by_name_and_cmdline("python", marker)

        assert [proc.pid for proc in killed] == [target.pid]
        assert target.wait(timeout=10) == -signal.SIGKILL
    finally:
        target.kill()
        target.wait()
        zombie.wait()


# A tracking service of the deployment's own, beyond firexapp's flame, keeper and blaze.
_EXTRA_TRACKING_SERVICE = "acme_tracking"

# What a submit starts, and what it leaves defunct behind it when it hangs.
_TRACKING_SERVICE_NAMES = (
    "firex_flame",
    "firex_keeper",
    "firex_blaze",
    _EXTRA_TRACKING_SERVICE,
)

# A stand-in for 'firexapp submit': it starts the tracking services named in its
# arguments and then, exactly like a submit stuck in its shutdown path, never reaps
# them. Nothing in firexapp ever would either -- popen_tracking_service_subproc
# discards the Popen it returns -- so they stay defunct for as long as it does.
_HUNG_SUBMIT_SRC = """
import ctypes, os, sys, time
for name in sys.argv[2:]:
    if os.fork() == 0:
        # PR_SET_NAME: a defunct proc keeps its name, and that is all a FireX
        # tracking service has left once it has exited.
        ctypes.CDLL('libc.so.6').prctl(15, ctypes.c_char_p(name.encode()), 0, 0, 0)
        os._exit(0)
time.sleep(600)
"""

_UT_FIREX_ID = "FireX-someuser-260101-000000-11111"


def _wait_for(predicate, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout}s waiting for {predicate}")


def _proc_info(proc: psutil.Process) -> _OsProcInfo:
    info = FireXProc.get_localhost_proc_info(proc, time.time())
    assert info is not None, f"{proc} went away"
    return info


def _defunct_children(submit: psutil.Process) -> dict[str, _OsProcInfo]:
    infos = [_proc_info(child) for child in submit.children()]
    return {info.name: info for info in infos if info.is_defunct()}


def _firex_proc(info: _OsProcInfo, firex_id=_UT_FIREX_ID, os_proc=...) -> FireXProc:
    return FireXProc.create_from_proc_info(
        info,
        psutil.Process(info.pid) if os_proc is ... else os_proc,
        "localhost",
        firex_id=firex_id,
    )


@pytest.fixture(name="conventions")
def conventions_fixture(tmp_path, monkeypatch) -> FireXProcConventions:
    """A deployment with a tracking service and test env of its own, installed under
    tmp_path's root, so that a submit started from there is believed."""
    conventions = FireXProcConventions(
        extra_tracking_service_proc_names=(_EXTRA_TRACKING_SERVICE,),
        test_descendant_env_keys=("ACME_TEST_NAME",),
        exe_base=exe_base_of(str(tmp_path / "firex_procs.py")),
    )
    monkeypatch.setattr(firex_procs, "_conventions", conventions)
    return conventions


@pytest.fixture(name="hung_submit")
def hung_submit_fixture(tmp_path, conventions) -> psutil.Process:
    """A real 'firexapp submit' process sitting on real defunct tracking services."""
    python = tmp_path / "python"  # so its name is 'python', like a real submit's.
    python.symlink_to(sys.executable)
    firexapp_script = tmp_path / "firexapp"
    firexapp_script.write_text(_HUNG_SUBMIT_SRC, encoding="utf-8")

    popen = subprocess.Popen(
        [str(python), str(firexapp_script), "submit", *_TRACKING_SERVICE_NAMES]
    )
    submit = psutil.Process(popen.pid)
    try:
        _wait_for(
            lambda: len(_defunct_children(submit)) == len(_TRACKING_SERVICE_NAMES)
        )
        yield submit
    finally:
        if submit.is_running():
            submit.kill()
        popen.wait()


def test_psutil_reports_a_defunct_procs_identity_but_not_its_cmdline(hung_submit):
    defunct = _defunct_children(hung_submit)["firex_flame"]

    assert defunct.is_defunct()
    assert defunct.status == psutil.STATUS_ZOMBIE
    # What survives: who it was, and who still owes it a wait().
    assert defunct.ppid == hung_submit.pid
    assert defunct.username == getuser()
    assert defunct.get_age() > 0
    # ...but not either of the two things a FireX ID is ever read from. as_dict turns
    # the AccessDenied that environ raises for a defunct proc in to None.
    assert defunct.cmdline == []
    assert defunct.environ is None


def test_a_defunct_tracking_service_is_typed_but_cannot_name_its_own_run(hung_submit):
    defunct_by_name = _defunct_children(hung_submit)

    assert {
        name: info.get_firex_proc_type() for name, info in defunct_by_name.items()
    } == {
        "firex_flame": FireXProcType.FIREX_FLAME,
        "firex_keeper": FireXProcType.FIREX_KEEPER,
        "firex_blaze": FireXProcType.FIREX_BLAZE,
        _EXTRA_TRACKING_SERVICE: FireXProcType.FIREX_TRACKING_SERVICE,
    }
    # so each is killable on leak, but only once something else names its run.
    assert all(
        info.get_firex_proc_type().should_kill_on_leak()
        for info in defunct_by_name.values()
    )
    assert not any(info.is_firex_proc() for info in defunct_by_name.values())


def test_tracking_services_beyond_firexapps_own_need_the_deployments_conventions(
    hung_submit, conventions, monkeypatch
):
    assert FireXProcType.FIREX_TRACKING_SERVICE in (
        FireXProcType.get_inprogress_run_proc_types()
    )

    monkeypatch.setattr(
        firex_procs,
        "_conventions",
        dataclasses.replace(conventions, extra_tracking_service_proc_names=()),
    )

    extra = _defunct_children(hung_submit)[_EXTRA_TRACKING_SERVICE]
    assert extra.get_firex_proc_type() is FireXProcType.UNKNOWN
    # nor can a run in progress be expected to have one.
    assert FireXProcType.FIREX_TRACKING_SERVICE not in (
        FireXProcType.get_inprogress_run_proc_types()
    )


def test_a_submit_is_recognised_by_its_cmdline_and_names_no_run_itself(hung_submit):
    submit = _proc_info(hung_submit)

    assert submit.name == "python"  # nothing to go on there.
    assert submit.is_firex_submit_cmd()
    assert submit.get_firex_proc_type() is FireXProcType.FIREX_MAIN
    assert not submit.is_firex_proc()


def test_a_submit_outside_the_install_root_is_not_a_submit(
    hung_submit, conventions, monkeypatch
):
    # Someone else's submit is none of our business, and killing it would be
    # considerably worse than missing it.
    monkeypatch.setattr(
        firex_procs,
        "_conventions",
        dataclasses.replace(conventions, exe_base="/some/other/root/"),
    )

    assert not _proc_info(hung_submit).is_firex_submit_cmd()


def test_a_hung_submit_and_its_defunct_procs_are_found_under_one_id(hung_submit):
    runs = FireXProcQuery(
        query_username=getuser(),
        query_recursive_children=True,
    ).query_procs_per_run()

    found = {
        firex_id: run.firex_procs
        for firex_id, run in runs.items()
        if any(fp.proc_info.pid == hung_submit.pid for fp in run.firex_procs)
    }

    assert len(found) == 1, f"submit should belong to exactly one run, got {found}"
    ((firex_id, firex_procs),) = found.items()
    # Nothing is left to name the run, so it gets an ID of its own -- carrying the pid
    # of the one process still there, so it is traceable and cannot collide.
    assert firex_id.startswith("FireX-UNKNOWNFIREXID-")
    assert firex_id.endswith(f"-{hung_submit.pid}")
    assert {fp.proc_info.name for fp in firex_procs} == {
        "python",
        *_TRACKING_SERVICE_NAMES,
    }
    assert {fp.proc_info.pid for fp in firex_procs if fp.is_defunct()} == {
        info.pid for info in _defunct_children(hung_submit).values()
    }


def test_a_defunct_proc_cannot_be_killed(hung_submit):
    flame = _firex_proc(_defunct_children(hung_submit)["firex_flame"])

    assert flame.is_defunct()
    assert flame.kill() is False
    # It was already dead, so there was nothing a signal could do to it.
    assert psutil.Process(flame.proc_info.pid).status() == psutil.STATUS_ZOMBIE


def test_killing_a_hung_submit_reaps_the_defunct_procs_it_was_holding(hung_submit):
    firex_procs_to_kill = [
        _firex_proc(info)
        for info in [_proc_info(hung_submit), *_defunct_children(hung_submit).values()]
    ]

    started = time.time()
    killed = force_kill_firex_procs(firex_procs_to_kill)
    took = time.time() - started

    # The defunct ones go too, because init reaps them once the submit that owed them
    # a wait() is gone.
    assert {fp.proc_info.pid for fp in killed} == {
        fp.proc_info.pid for fp in firex_procs_to_kill
    }
    # Waiting on a defunct proc that is not this process' own child would have burned
    # the full force-kill timeout and then reported it still alive.
    assert took < 5, f"force kill took {took}s"


def test_a_hung_submit_is_idle_but_one_still_working_is_not(hung_submit):
    busy = subprocess.Popen([sys.executable, "-c", "while True: pass"])
    try:
        assert _firex_proc(_proc_info(hung_submit)).is_idle_after(0.5)
        assert not _firex_proc(_proc_info(psutil.Process(busy.pid))).is_idle_after(0.5)
    finally:
        busy.kill()
        busy.wait()


def test_a_submit_that_exits_while_settling_is_not_idle():
    popen = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"])
    firex_proc = _firex_proc(_proc_info(psutil.Process(popen.pid)))
    try:
        # It got on with its shutdown, so there is nothing left to call hung.
        assert not firex_proc.is_idle_after(2)
    finally:
        popen.wait()


def test_only_a_submit_whose_every_firex_child_is_defunct_is_a_candidate(hung_submit):
    submit = _firex_proc(_proc_info(hung_submit))
    defunct = [_firex_proc(info) for info in _defunct_children(hung_submit).values()]
    live_celery = _firex_proc(
        _OsProcInfo(
            pid=hung_submit.pid + 10**6,  # any pid that isn't one of the above.
            ppid=hung_submit.pid,
            name="celery",
            cmdline=["celery", "--uid", _UT_FIREX_ID],
            status=psutil.STATUS_SLEEPING,
            username=getuser(),
            elapsed_time=defunct[0].get_age(),
            now=time.time(),
        ),
        os_proc=None,  # no such process: only its place in the topology matters here.
    )

    assert FireXProc.get_submits_with_only_defunct_children(
        [submit, *defunct],
        min_age=0,
    ) == [submit]
    assert not FireXProc.get_submits_with_only_defunct_children(
        [submit, *defunct, live_celery],
        min_age=0,
    )
    assert not FireXProc.get_submits_with_only_defunct_children(
        [submit],
        min_age=0,  # a submit with no children at all is just working.
    )
    assert not FireXProc.get_submits_with_only_defunct_children(
        [submit, *defunct],
        min_age=60 * 60,
    )


_CELERY_DESCENDANT_ENV = {
    "CURRENT_RUN_FIREX_ID": _UT_FIREX_ID,
    "CELERY_LOG_FILE": f"/logs/{_UT_FIREX_ID}/microservice_logs/worker@a-host.html",
}


def test_a_celery_descendant_is_a_test_proc_only_by_the_deployments_env(conventions):
    assert FireXProcType.get_type_and_fid_from_env(_CELERY_DESCENDANT_ENV) == (
        FireXProcType.FIREX_RUN_CELERY_DESCENDANT,
        _UT_FIREX_ID,
    )
    assert FireXProcType.get_type_and_fid_from_env(
        _CELERY_DESCENDANT_ENV | {"ACME_TEST_NAME": "a-test"}
    ) == (FireXProcType.FIREX_RUN_CELERY_TEST_DESCENDANT, _UT_FIREX_ID)


def test_without_test_env_no_celery_descendant_is_a_test_proc(monkeypatch):
    # all() of no keys is True, which must not make every descendant a test proc.
    monkeypatch.setattr(firex_procs, "_conventions", FireXProcConventions())

    assert FireXProcType.get_type_and_fid_from_env(_CELERY_DESCENDANT_ENV) == (
        FireXProcType.FIREX_RUN_CELERY_DESCENDANT,
        _UT_FIREX_ID,
    )


def test_querying_another_host_needs_a_remote_cmd_runner(monkeypatch):
    monkeypatch.setattr(firex_procs, "_conventions", FireXProcConventions())

    with pytest.raises(RemoteFireXProcException, match="no remote command runner"):
        FireXProcQuery().query_firex_procs(hostname="some-other-host")
