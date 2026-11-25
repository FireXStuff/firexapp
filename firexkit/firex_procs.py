"""Finding the processes of FireX runs, and which run each belongs to.

A FireX run is spread over many processes -- the broker, celery workers, tracking
services, the submit itself and anything those start -- and most of them can only
be tied back to their run by a FireX ID somewhere in their cmdline or environ. This
module finds them by that ID, types them, and kills them.

What a deployment adds to the processes firexapp itself starts (its own submit
wrapper, extra tracking services, how test processes are marked, how to reach
other hosts) is described by FireXProcConventions; see set_firex_proc_conventions.
"""

import dataclasses
import enum
import logging
import os.path
import re
import shlex
import socket
import time
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from getpass import getuser
from typing import Any, Optional

import psutil

from firexapp.submit.console import setup_console_logging
from firexapp.submit.uid import find_all_firex_ids_from_str, firex_id_str

logger = setup_console_logging(__name__)


class RemoteFireXProcException(Exception):
    """The remote host could not be reached at all."""


class RemoteFireXProcCmdFailed(Exception):
    """The remote host was reached, but the command exited non-zero."""


# Runs cmd on hostname and returns its stdout:
# (hostname, cmd, connect_retries, cmd_retries) -> stdout
# Must raise RemoteFireXProcException when the host can't be reached, and
# RemoteFireXProcCmdFailed when the command exits non-zero.
RemoteCmdRunner = Callable[[str, str | list[str], int, int], str]

# Logs a single kill event at the given level.
KillLogFn = Callable[[int, str], None]


def _no_remote_cmd_runner(hostname: str, cmd, connect_retries, cmd_retries) -> str:
    raise RemoteFireXProcException(
        f"Cannot query {hostname}: no remote command runner is configured "
        "(see FireXProcConventions.remote_cmd_runner)."
    )


@dataclasses.dataclass(frozen=True)
class FireXProcConventions:
    """What identifies a deployment's FireX processes beyond what firexapp itself starts.

    The defaults describe a plain firexapp install. A deployment with its own submit
    wrapper, tracking services or test environment registers its own conventions with
    set_firex_proc_conventions before querying.
    """

    # The process name of what a user runs to submit, e.g. a console script.
    main_proc_names: tuple[str, ...] = ("firexapp",)
    # The basename of a python script run as '<python> <script> submit'.
    submit_script_names: tuple[str, ...] = ("firexapp",)
    # Tracking services beyond firexapp's own (flame, keeper, blaze), by process name.
    extra_tracking_service_proc_names: tuple[str, ...] = ()
    # Env keys that, when all set, mark a celery worker's descendant as a test process.
    test_descendant_env_keys: tuple[str, ...] = ()
    # How old an infra process that no longer names its run has to be before it is
    # given a made-up FireX ID, so that leak detection can still find it.
    extremely_old_run_age: float = timedelta(weeks=4, days=2).total_seconds()
    # The install root processes must run from for a 'submit' to be believed, e.g.
    # '/auto/firex/'. None means the root this module was imported from.
    exe_base: str | None = None
    remote_cmd_runner: RemoteCmdRunner = _no_remote_cmd_runner

    def get_exe_base(self) -> str | None:
        if self.exe_base is not None:
            return self.exe_base
        return exe_base_of(__file__)


def exe_base_of(path: str) -> str | None:
    """The install root of path, e.g. /auto/firex/ for /auto/firex/.../site-packages/x.py"""
    base_parts = path.split("/")[:3]
    if len(base_parts) == 3:
        return "/".join(base_parts) + "/"
    return None


_conventions = FireXProcConventions()


def set_firex_proc_conventions(conventions: FireXProcConventions) -> None:
    global _conventions
    _conventions = conventions


def get_firex_proc_conventions() -> FireXProcConventions:
    return _conventions


def _flatten(lists: Iterable[list[Any]]) -> list[Any]:
    return [item for sublist in lists for item in sublist]


def _get_celery_destination_from_log_path(maybe_log_path: str) -> str | None:
    m = re.match(r".*/(?P<celery_destination>\S+?).html$", maybe_log_path)
    if m:
        return m.groupdict()["celery_destination"]
    return None


@enum.unique
class FireXProcType(enum.Enum):
    # values name the type, and are the process name of infra procs whose name
    # doesn't depend on the deployment -- see get_proc_names.
    REDIS_SERVER = "redis-server"
    CELERY = "celery"
    # What a user runs to submit, e.g: '/bin/bash /auto/firex/bin/firex submit ...'.
    # Its process name is a deployment convention.
    FIREX_MAIN = "firex"
    FIREX_KEEPER = "firex_keeper"
    FIREX_FLAME = "firex_flame"
    FIREX_BLAZE = "firex_blaze"
    # Any tracking service a deployment adds, named by its conventions.
    FIREX_TRACKING_SERVICE = "firex_tracking_service"
    FIREX_SHUTDOWN = "firex_shutdown"
    FIREX_RUN_DESCENDANT = "FIREX_RUN_DESCENDANT"
    FIREX_RUN_CELERY_DESCENDANT = "FIREX_CELERY_DESCENDANT"
    FIREX_RUN_CELERY_TEST_DESCENDANT = "FIREX_TEST_CELERY_DESCENDANT"

    UNKNOWN = "UNKNOWN"  # a proc with a FireX ID in its cmd that isn't any of the known types.

    def _get_details(self) -> "FireXProcTypeDetails":
        return _PROC_TYPE_TO_DETAILS[self]

    def is_infra_process(
        self,
        tracking_service: bool | None = None,
        firex_main: bool | None = None,
    ) -> bool:
        if any(q is not None for q in [tracking_service, firex_main]):
            # specific infra proc query.
            return bool(
                tracking_service is not None
                and self._get_details().from_tracking_service == tracking_service
                or firex_main is not None
                and (self == FireXProcType.FIREX_MAIN) == firex_main
            )

        return self._get_details().is_infra_proc  # any infra proc query.

    def should_kill_on_leak(self) -> bool:
        return self._get_details().kill_on_leak

    def get_proc_names(self) -> tuple[str, ...]:
        if not self.is_infra_process():
            # we don't know the expected process name.
            return ()
        if self == FireXProcType.FIREX_MAIN:
            return get_firex_proc_conventions().main_proc_names
        if self == FireXProcType.FIREX_TRACKING_SERVICE:
            return get_firex_proc_conventions().extra_tracking_service_proc_names
        return (self.value,)

    def get_celery_destination(
        self,
        cmd: list[str],
        environ: dict[str, str] | None,
    ) -> str | None:
        if self._get_details().has_celery_destination:
            if self == FireXProcType.CELERY:
                container = cmd
            elif environ and self in [
                FireXProcType.FIREX_RUN_CELERY_DESCENDANT,
                FireXProcType.FIREX_RUN_CELERY_TEST_DESCENDANT,
            ]:
                container = [
                    environ.get(k, "")
                    for k in FireXProcType.FIREX_RUN_CELERY_DESCENDANT.get_firex_id_env_keys()
                ]
            else:
                container = []

            for opt in container:
                maybe_destination = _get_celery_destination_from_log_path(opt)
                if maybe_destination:
                    return maybe_destination

        return None

    def get_firex_id_env_keys(self) -> tuple[str, ...]:
        return self._get_details().firex_id_env_keys

    def fids_from_env(self, environ: dict[str, str]) -> list[str]:
        return _flatten(
            find_all_firex_ids_from_str(environ.get(k, ""))
            for k in self.get_firex_id_env_keys()
        )

    @classmethod
    def get_type_and_fid_from_env(
        cls,
        environ: dict[str, str],
    ) -> tuple[
        Optional["FireXProcType"],
        str | None,
    ]:
        run_nvars_firex_ids = cls.FIREX_RUN_DESCENDANT.fids_from_env(environ)
        if len(run_nvars_firex_ids) == 1:
            # the process is at least a FIREX_RUN_DESCENDANT, see if we can
            # narrow to FIREX_RUN_CELERY_TEST_DESCENDANT (most specific)
            # or FIREX_RUN_CELERY_DESCENDANT
            run_nvars_firex_id = run_nvars_firex_ids[0]
            celery_nvars_firex_ids = cls.FIREX_RUN_CELERY_DESCENDANT.fids_from_env(
                environ
            )
            if len(celery_nvars_firex_ids) == 1:
                celery_nvars_firex_id = celery_nvars_firex_ids[0]
                if celery_nvars_firex_id == run_nvars_firex_id:
                    test_env_keys = (
                        get_firex_proc_conventions().test_descendant_env_keys
                    )
                    # all() of no keys is True, which would make every celery
                    # descendant a test proc in a deployment that marks none.
                    is_test_proc = bool(test_env_keys) and all(
                        environ.get(k) for k in test_env_keys
                    )
                    if is_test_proc:
                        return cls.FIREX_RUN_CELERY_TEST_DESCENDANT, run_nvars_firex_id
                    else:
                        return cls.FIREX_RUN_CELERY_DESCENDANT, run_nvars_firex_id
                else:
                    logger.warning(
                        f"Found mismatch between current run firex ID {run_nvars_firex_id} "
                        f"and celery proc firex ID {celery_nvars_firex_id}. Ignoring celery proc firex ID."
                    )

            return cls.FIREX_RUN_DESCENDANT, run_nvars_firex_id

        return None, None

    @staticmethod
    def get_inprogress_run_proc_types() -> set["FireXProcType"]:
        return {
            t
            for t in FireXProcType
            if t._get_details().implies_inprogress
            # a type no process of this deployment is ever named after can't be
            # expected of a run in progress.
            and (not t.is_infra_process() or t.get_proc_names())
        }

    @staticmethod
    def get_proc_firex_type_and_fids(
        os_proc: "_OsProcInfo",
    ) -> tuple["FireXProcType", list[str]]:
        for t in FireXProcType:
            if any(
                type_proc_name == os_proc.name
                # ps truncates process names (even with col width
                # adjusting options like -ww), so if a proc name
                # is long enough and a type's proc name starts with it,
                # consider it a match
                or (len(os_proc.name) > 10 and type_proc_name.startswith(os_proc.name))
                for type_proc_name in t.get_proc_names()
            ):
                # only consider firex_ids in cmdline for known infra procs.
                if t not in [FireXProcType.FIREX_MAIN, FireXProcType.UNKNOWN]:
                    # spaces can't introduce false firex ID matches.
                    firex_ids = find_all_firex_ids_from_str(" ".join(os_proc.cmdline))
                else:
                    firex_ids = FireXProcType.FIREX_RUN_DESCENDANT.fids_from_env(
                        os_proc.environ or {}
                    )

                if (
                    not firex_ids
                    and os_proc.get_age()
                    > get_firex_proc_conventions().extremely_old_run_age
                    # conservatively only treat known tracking_service and firex_main processes
                    # as belonging to an unknown, fake firex ID.
                    and t.is_infra_process(tracking_service=True, firex_main=True)
                    # don't kill "firex" from some other path root, someone else might use that script name (e.g. Github team)
                    and os_proc.is_common_exe_base()
                ):
                    # extremely old infra processes that no longer reference a FireX ID
                    # still need to be found (e.g. for leak detection), so they get a fake ID.
                    # We could check self's open_files to see if /tmp/firexsubmit...
                    # is open, then get the firex ID from there, but that's likely expensive,
                    # and the submit log may be deleted for very old runs anyways.
                    firex_ids = [
                        _create_unknown_firex_id(os_proc.get_age(), os_proc.pid)
                    ]

                # Note a defunct process can reach here with no firex_ids and get none:
                # it has lost its cmdline and environ, and is_common_exe_base() needs a
                # cmdline, so the fake ID above is out of reach by construction. That's
                # deliberate -- a defunct process always still has a live parent (if the
                # parent had died, init would have reaped it), so _query_cur_host_procs
                # attributes it to that parent's FireX ID instead, which keeps it grouped
                # with the rest of its run rather than under an ID of its own.
                return t, firex_ids

        if os_proc.is_firex_submit_cmd():
            # A '<script> submit' proc (e.g. 'firex.py submit') never names its own run:
            # its proc name is 'python', its cmdline carries the chain instead of a FireX
            # ID, and the FireX ID is only put in its environment after exec (e.g. as
            # CURRENT_RUN_FIREX_ID and firex_logs_dir), so /proc/PID/environ -- an
            # exec-time snapshot -- never shows it. Any FireX ID that *is* in its environ
            # names an outer run that launched this submit, not the run this submit owns,
            # so deliberately return no ID and let _query_cur_host_procs resolve it from
            # the procs this one spawned.
            return FireXProcType.FIREX_MAIN, []

        fids: list[str] = []
        # no match by proc name, try by environ
        if os_proc.environ:
            type_from_env, env_firex_id = FireXProcType.get_type_and_fid_from_env(
                os_proc.environ
            )
            if type_from_env:
                if env_firex_id:
                    fids.append(env_firex_id)
                return type_from_env, fids

        return FireXProcType.UNKNOWN, fids


@dataclasses.dataclass
class FireXProcTypeDetails:
    is_infra_proc: bool
    implies_inprogress: bool  # get_inprogress_run_proc_types
    has_celery_destination: bool = False
    from_tracking_service: bool = False
    kill_on_leak: bool = True
    firex_id_env_keys: tuple[str, ...] = ("CURRENT_RUN_FIREX_ID", "firex_logs_dir")


TRACKING_SERVICE_TYPE_DETAILS = FireXProcTypeDetails(
    is_infra_proc=True,
    implies_inprogress=True,
    from_tracking_service=True,
)

# Theres are procs that have a firex ID in the cmd or env,
# but we don't know much about them (not infra procs and
# not descendants of a firex celery proc), so we're very
# conservative with how we handle them.
NONE_PROC_TYPE_DETAILS = FireXProcTypeDetails(
    is_infra_proc=False,
    implies_inprogress=False,
    kill_on_leak=False,
)
CELERY_DESCENDANT_TYPE_DETAILS = FireXProcTypeDetails(
    is_infra_proc=False,
    has_celery_destination=True,
    implies_inprogress=False,
)

_PROC_TYPE_TO_DETAILS: dict[FireXProcType, FireXProcTypeDetails] = {
    FireXProcType.FIREX_MAIN: FireXProcTypeDetails(
        is_infra_proc=True,
        kill_on_leak=True,
        # flow test infra doesn't allow testing non-sync part way through.
        implies_inprogress=False,
    ),
    FireXProcType.REDIS_SERVER: FireXProcTypeDetails(
        is_infra_proc=True,
        implies_inprogress=True,  # get_inprogress_run_proc_types
    ),
    FireXProcType.CELERY: FireXProcTypeDetails(
        is_infra_proc=True,
        has_celery_destination=True,
        implies_inprogress=True,
    ),
    FireXProcType.FIREX_SHUTDOWN: FireXProcTypeDetails(
        is_infra_proc=True,
        implies_inprogress=False,
    ),
    FireXProcType.FIREX_RUN_DESCENDANT: NONE_PROC_TYPE_DETAILS,
    FireXProcType.FIREX_RUN_CELERY_DESCENDANT: dataclasses.replace(
        CELERY_DESCENDANT_TYPE_DETAILS,
        firex_id_env_keys=("CELERY_LOG_FILE", "_MP_FORK_LOGFILE_"),
    ),
    # what marks one as a test proc is FireXProcConventions.test_descendant_env_keys
    FireXProcType.FIREX_RUN_CELERY_TEST_DESCENDANT: dataclasses.replace(
        CELERY_DESCENDANT_TYPE_DETAILS,
        firex_id_env_keys=("CELERY_LOG_FILE", "_MP_FORK_LOGFILE_"),
    ),
    FireXProcType.UNKNOWN: NONE_PROC_TYPE_DETAILS,
} | {
    t: TRACKING_SERVICE_TYPE_DETAILS
    for t in [
        FireXProcType.FIREX_KEEPER,
        FireXProcType.FIREX_FLAME,
        FireXProcType.FIREX_BLAZE,
        FireXProcType.FIREX_TRACKING_SERVICE,
    ]
}


def _create_unknown_firex_id(age_secs: float, pid: int) -> str:
    fake_start_time = datetime.now(tz=timezone.utc) - timedelta(seconds=age_secs)
    # the pid goes where a real FireX ID has a random int, so that two procs that
    # can't name their run don't collide into one fake ID just by being the same age.
    return firex_id_str("UNKNOWNFIREXID", fake_start_time, pid)


_PROC_KEYS = [
    "pid",
    "ppid",
    "create_time",
    "username",
    "name",
    "status",
    "cmdline",
    "environ",
]


@dataclasses.dataclass
class _OsProcInfo:
    pid: int
    username: str
    name: str
    cmdline: list[str]
    now: float
    environ: dict[str, str] | None = None

    # psutil reads both of these out of the /proc/PID/stat parse it already does
    # for 'name', so they're free here. The remote (ps) path leaves them None;
    # it greps cmdline for a FireX ID, which a defunct proc never has.
    ppid: int | None = None
    status: str | None = None

    # must have one or other so that age can be determined.
    create_time: float | None = None
    elapsed_time: float | None = None

    # calculated
    _firex_ids: list[str] | None = None
    _firex_proc_type: FireXProcType | None = None

    def __post_init__(self):
        assert (self.create_time is not None) ^ (self.elapsed_time is not None), (
            "must have either start time or elapsed time"
        )

        if self.create_time is None:
            assert self.elapsed_time is not None
            self.create_time = self.now - self.elapsed_time

        if self.elapsed_time is None:
            assert self.create_time is not None
            self.elapsed_time = self.now - self.create_time

    def get_age(self) -> float:
        if self.elapsed_time is not None:
            return self.elapsed_time
        assert self.create_time is not None

        return time.time() - self.create_time

    def is_common_exe_base(self) -> bool:
        if self.cmdline:
            # e.g. check cmdline startswith /auto/firex/
            exe_base = get_firex_proc_conventions().get_exe_base()
            if exe_base:
                return self.cmdline[0].startswith(exe_base)
        return False

    def is_firex_submit_cmd(self) -> bool:
        """Is this the python process running '<submit script> submit', e.g. 'firex.py submit'?"""
        return bool(
            # e.g. /auto/firex/STAGINGS/STAGING_41007/firex/venv//bin/python
            #      /auto/firex/.../site-packages/firex_cisco//firex.py submit --chain ...
            len(self.cmdline) > 2
            # don't claim a firex.py from some other path root, someone else might
            # use that script name. This also excludes the runInVenv bash wrapper,
            # whose argv[0] is /bin/bash.
            and self.is_common_exe_base()
            and os.path.basename(self.cmdline[1])
            in get_firex_proc_conventions().submit_script_names
            and self.cmdline[2] == "submit"
        )

    def is_defunct(self) -> bool:
        """Has this process exited without its parent having wait()ed on it yet?

        A defunct (zombie) process keeps its pid, ppid, create_time, username and
        name, but has lost its cmdline (empty) and environ (AccessDenied, which
        Process.as_dict turns in to None). It cannot be killed -- it is already
        dead -- and only goes away once its parent reaps it or dies itself.
        """
        return self.status == psutil.STATUS_ZOMBIE

    def get_firex_id(self) -> str:
        assert self._firex_ids, "Check is_firex_proc before getting firex ID."
        return self._firex_ids[0]

    def get_firex_proc_type_and_fids(self) -> tuple[FireXProcType, list[str]]:
        if self._firex_proc_type is None or self._firex_ids is None:
            proc_type, firex_ids = FireXProcType.get_proc_firex_type_and_fids(self)
            if self._firex_proc_type is None:
                self._firex_proc_type = proc_type

            if self._firex_ids is None:
                self._firex_ids = firex_ids

        return self._firex_proc_type, self._firex_ids

    def get_firex_proc_type(self) -> FireXProcType:
        return self.get_firex_proc_type_and_fids()[0]

    def is_firex_proc(self) -> bool:
        proc_type, firex_ids = self.get_firex_proc_type_and_fids()
        if firex_ids:
            fids_count = len(firex_ids)
            if fids_count == 1 or proc_type == FireXProcType.UNKNOWN:
                return True
            elif fids_count > 1:
                logger.warning(
                    f"Found multiple FireX IDs {','.join(firex_ids)} in cmdline and/or venv: {self.cmdline}"
                )
        return False

    @staticmethod
    def create_from_psutil_dict(
        psutil_info: dict[str, Any], now: float
    ) -> "_OsProcInfo":
        # could assert _PROC_KEYS in proc_info
        return _OsProcInfo(now=now, **psutil_info)

    @staticmethod
    def get_ps_cmd() -> str:
        return "/bin/ps -e --format pid=,etimes=,user=,comm=,command= | /usr/bin/fgrep FireX-"

    @staticmethod
    def create_from_ps_output(ps_output: str, now: float) -> list["_OsProcInfo"]:
        processes = []
        for line in ps_output.splitlines():
            if line:
                fields = line.split(maxsplit=4)
                if len(fields) == 5:
                    try:
                        processes.append(
                            _OsProcInfo(
                                pid=int(fields[0]),
                                elapsed_time=float(fields[1]),
                                username=fields[2],
                                name=fields[3],
                                cmdline=shlex.split(fields[4]),
                                now=now,
                            )
                        )
                    except ValueError as e:
                        logger.error(
                            f"cannot convert to process info {e}: {fields[:-1]}"
                        )
                else:
                    logger.warning(f"expected 6 fields found {len(fields)}")
        return processes


def _run_remote_cmd(
    hostname: str, cmd: str | list[str], connect_retries=3, cmd_retries=0
) -> str:
    return get_firex_proc_conventions().remote_cmd_runner(
        hostname, cmd, connect_retries, cmd_retries
    )


@dataclasses.dataclass
class FireXProc:
    firex_id: str
    firex_proc_type: FireXProcType
    proc_info: _OsProcInfo
    hostname: str
    celery_destination: str | None

    os_proc: psutil.Process | None = None

    def get_age(self):
        return self.proc_info.get_age()

    def __str__(self) -> str:
        return f"{self.proc_info.name} with pid {self.proc_info.pid} [{self.firex_id}]"

    @staticmethod
    def create_from_proc_info(
        proc_info: _OsProcInfo,
        os_proc: psutil.Process | None,
        hostname: str,
        # wherever possible, this should be determined from process details.
        # but sometimes we only know a process is a firex process because it is a child of a firex process,
        # in which case we only know the firex ID from the ancestor process instead of from the process itself.
        firex_id: str | None = None,
    ) -> "FireXProc":
        if firex_id is None:
            assert proc_info.is_firex_proc(), f"Non-firex process: {proc_info.cmdline}"
            firex_id = proc_info.get_firex_id()

        firex_proc_type = proc_info.get_firex_proc_type()
        return FireXProc(
            os_proc=os_proc,
            firex_id=firex_id,
            firex_proc_type=firex_proc_type,
            proc_info=proc_info,
            hostname=hostname,
            celery_destination=firex_proc_type.get_celery_destination(
                proc_info.cmdline, proc_info.environ
            ),
        )

    @staticmethod
    def get_localhost_proc_info(proc: psutil.Process, now) -> _OsProcInfo | None:
        try:
            proc_info_dict = proc.as_dict(attrs=_PROC_KEYS)
        except psutil.NoSuchProcess:
            pass
        except IndexError as e:
            # psutil can fail with an IndexError when attempting to inspect some username values, such as nginx
            # worker processes launched via docker/supodman.
            logger.warning(f"Failure trying to read process details: {e}")
        else:
            return _OsProcInfo.create_from_psutil_dict(proc_info_dict, now)
        return None

    @staticmethod
    def create_from_localhost_proc(proc: psutil.Process, now) -> Optional["FireXProc"]:
        proc_info = FireXProc.get_localhost_proc_info(proc, now)
        if proc_info and proc_info.is_firex_proc():
            return FireXProc.create_from_proc_info(proc_info, proc, "localhost")
        return None

    def is_defunct(self) -> bool:
        return self.proc_info.is_defunct()

    def wait(self, timeout=None):
        if self.os_proc:
            self.os_proc.wait(timeout)
        else:
            # remote wait.
            timeout_cmd = ["/bin/timeout", str(timeout)] if timeout else []
            try:
                _run_remote_cmd(
                    self.hostname,
                    timeout_cmd
                    + ["/bin/tail", f"--pid={self.proc_info.pid}", "-f", "/dev/null"],
                )
            except RemoteFireXProcCmdFailed as e:
                raise psutil.TimeoutExpired(
                    timeout,
                    self.proc_info.pid,
                    self.proc_info.name,
                ) from e

    def kill(self) -> bool:
        logger.debug(f"Killing {self}")
        if self.is_defunct():
            # Signalling a zombie does nothing -- it has already exited, and only
            # goes away once the parent that owes it a wait() reaps it or dies.
            logger.debug(f'------ NOT KILLING "{self}": process is already defunct')
        elif not self.os_proc:
            logger.debug(
                f'------ FAILED TO KILL "{self}": Can only kill processes on the current host'
            )
        else:
            try:
                self.os_proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                logger.debug(f"process {self.os_proc} cannot be killed: {e}")
            else:
                return True
        return False

    @staticmethod
    def get_procs_by_type(
        firex_procs: list["FireXProc"], query_proc_type: FireXProcType
    ) -> list["FireXProc"]:
        return [
            firex_proc
            for firex_proc in firex_procs
            if firex_proc.firex_proc_type == query_proc_type
        ]

    @classmethod
    def _get_leaked_celery_descendants_by_celery_destination(
        cls, firex_procs: list["FireXProc"]
    ) -> dict[str, list["FireXProc"]]:
        celery_procs_and_descs_by_celery_dest: dict[str, list[FireXProc]] = {}
        for firex_proc in firex_procs:
            if firex_proc.celery_destination:
                if (
                    firex_proc.celery_destination
                    not in celery_procs_and_descs_by_celery_dest
                ):
                    celery_procs_and_descs_by_celery_dest[
                        firex_proc.celery_destination
                    ] = []
                celery_procs_and_descs_by_celery_dest[
                    firex_proc.celery_destination
                ].append(firex_proc)

        return {
            celery_destination: celery_procs
            for celery_destination, celery_procs in celery_procs_and_descs_by_celery_dest.items()
            # if a run has no Celery procs for a destination, but some procs P were launched
            # from that celery destination, then P are leaked.
            if FireXProcType.CELERY not in {fp.firex_proc_type for fp in celery_procs}
        }

    @classmethod
    def has_leaked_celery_descendants(cls, firex_procs: list["FireXProc"]) -> bool:
        return bool(
            cls._get_leaked_celery_descendants_by_celery_destination(firex_procs)
        )

    @classmethod
    def get_submits_with_only_defunct_children(
        cls,
        firex_procs: list["FireXProc"],
        min_age: float,
    ) -> list["FireXProc"]:
        """FIREX_MAIN procs older than min_age whose every FireX child is defunct.

        A submit that is sitting on nothing but defunct children has already lost
        everything that ran its run -- the tracking services are the children, and
        Celery and the broker are reparented to init rather than being children at
        all, so they'd have to be dead too for the run to be over.

        This is a candidate, not a verdict: see is_idle_after.
        """
        pids = {fp.proc_info.pid for fp in firex_procs}
        children_by_ppid: dict[int, list[FireXProc]] = {}
        for fp in firex_procs:
            ppid = fp.proc_info.ppid
            if ppid is not None and ppid in pids:
                children_by_ppid.setdefault(ppid, []).append(fp)

        return [
            submit
            for submit in cls.get_procs_by_type(firex_procs, FireXProcType.FIREX_MAIN)
            for children in [children_by_ppid.get(submit.proc_info.pid, [])]
            if children
            and submit.get_age() > min_age
            and all(child.is_defunct() for child in children)
        ]

    def is_idle_after(self, settle_secs: float) -> bool:
        """Is this process still here, and no further along, settle_secs from now?

        Nothing ever wait()s on the tracking services a submit starts -- firexapp's
        popen_tracking_service_subproc discards the Popen it returns -- so they stay
        defunct from the moment they exit until the submit itself exits and init
        reaps them. Every --sync run therefore ends with its submit sitting on four
        defunct children for as long as its final reporting takes, which is
        indistinguishable in a single sample from a submit that is never going to
        finish. Letting it settle tells the two apart: one writing its reports either
        exits or burns some CPU, and one that is hung does neither.

        Blocks for settle_secs, so only call it for a process already believed hung.
        """
        if not self.os_proc:
            return False
        try:
            before = self.os_proc.cpu_times()
            time.sleep(settle_secs)
            after = self.os_proc.cpu_times()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False  # it got on with it, nothing left to kill.
        return (before.user + before.system) == (after.user + after.system)


@dataclasses.dataclass
class _SingleRunFireXProcs:
    firex_id: str
    firex_procs: list["FireXProc"]


@dataclasses.dataclass
class FireXProcQuery:
    # default to query all procs.
    proc_types: list[FireXProcType] = dataclasses.field(
        default_factory=lambda: [t for t in FireXProcType]
    )
    query_min_age: float | None = None
    query_max_age: float | None = None
    query_username: str | None = None
    firex_id: str | None = None
    celery_worker: str | None = None  # e.g. worker2@ott-ads-962
    # consider_environ: Optional[bool] = None

    #
    # None - query no children processes
    # True - query recursive children processes
    # False - query children processes, but not recursive children.
    #
    # child procs must also match all other query parameters --
    # this option just controls if children are considered at all.
    query_recursive_children: bool | None = None

    def _matches(self, firex_proc: FireXProc) -> bool:
        return bool(
            (self.query_min_age is None or firex_proc.get_age() >= self.query_min_age)
            and (
                self.query_max_age is None or firex_proc.get_age() < self.query_max_age
            )
            and (
                self.query_username is None
                or self.query_username == firex_proc.proc_info.username
            )
            and (
                self.proc_types is None or firex_proc.firex_proc_type in self.proc_types
            )
            and (self.firex_id is None or self.firex_id == firex_proc.firex_id)
        )

    def query_remote_host_procs(
        self,
        hostname,
    ) -> list[FireXProc]:
        assert self.query_recursive_children is None, (
            "Querying remote child procs recursively is not yet supported."
        )
        stdout = _run_remote_cmd(
            hostname,
            # TODO: could update command based on
            # query (e.g. only current user, only one firex_proc_type,
            # etc)
            _OsProcInfo.get_ps_cmd(),
            connect_retries=1,
        )
        # FIXME: add support for getting environ from remote.

        now = time.time()
        rps = [
            FireXProc.create_from_proc_info(rp, None, hostname)
            for rp in _OsProcInfo.create_from_ps_output(stdout, now)
            if rp.is_firex_proc()
        ]
        return [fp for fp in rps if self._matches(fp)]

    def _query_cur_host_procs(self) -> list[FireXProc]:
        firex_procs_by_os_procs: dict[psutil.Process, FireXProc] = {}
        # every proc inspected below, so that _resolve_procs_from_relatives can work
        # out who is related to whom without inspecting anything a second time.
        proc_infos_by_pid: dict[int, tuple[psutil.Process, _OsProcInfo]] = {}
        now = time.time()
        for proc in psutil.process_iter():
            # process children and env queries can cause duplicate os_procs, so we check upfront to de-duplicate.
            if proc not in firex_procs_by_os_procs:
                proc_info = FireXProc.get_localhost_proc_info(proc, now)
                if proc_info is None:
                    continue
                proc_infos_by_pid[proc_info.pid] = (proc, proc_info)

                firex_proc = (
                    FireXProc.create_from_proc_info(proc_info, proc, "localhost")
                    if proc_info.is_firex_proc()
                    else None
                )
                if firex_proc and self._matches(firex_proc):
                    firex_procs_by_os_procs[proc] = firex_proc
                    if self.query_recursive_children is not None:
                        try:
                            # possible improvement: since we're already iterating over all procs, could sort by time
                            # then find children via firex_procs_by_os_procs booking keeping we're
                            # doing anyway. That saves double inspection.
                            child_os_procs = proc.children(
                                recursive=self.query_recursive_children
                            )
                        except psutil.NoSuchProcess:
                            pass
                        else:
                            for child_proc in child_os_procs:
                                if child_proc not in firex_procs_by_os_procs:
                                    child_proc_info = FireXProc.get_localhost_proc_info(
                                        child_proc, now
                                    )
                                    if child_proc_info is None:
                                        continue
                                    proc_infos_by_pid[child_proc_info.pid] = (
                                        child_proc,
                                        child_proc_info,
                                    )
                                    if child_proc_info.is_firex_proc():
                                        child_firex_proc = (
                                            FireXProc.create_from_proc_info(
                                                child_proc_info, child_proc, "localhost"
                                            )
                                        )
                                        if self._matches(child_firex_proc):
                                            firex_procs_by_os_procs[child_proc] = (
                                                child_firex_proc
                                            )

        self._resolve_procs_from_relatives(firex_procs_by_os_procs, proc_infos_by_pid)

        return sorted(
            firex_procs_by_os_procs.values(),
            key=lambda fp: fp.proc_info.get_age(),
        )

    def _resolve_procs_from_relatives(
        self,
        firex_procs_by_os_procs: dict[psutil.Process, FireXProc],
        proc_infos_by_pid: dict[int, tuple[psutil.Process, _OsProcInfo]],
    ) -> None:
        """Find the FireX procs that can only be identified by who they are related to.

        Two kinds of proc never name their own run, so the pass above drops both:

          - A 'firex.py submit' proc: see FireXProcType.get_proc_firex_type_and_fids.
            Its children were started with --uid, so they can name it for it.
          - A defunct proc: it has lost its cmdline and environ, which is everything
            that FireX ID matching reads. Its parent can name it instead -- a defunct
            proc always still has a live parent, since if the parent had died the proc
            would have been reparented to init and reaped.

        Resolution is from direct children/parents only, so each step is one hop from
        a proc that identified itself.
        """
        children_by_ppid: dict[int, list[int]] = {}
        for pid, (_, proc_info) in proc_infos_by_pid.items():
            if proc_info.ppid is not None:
                children_by_ppid.setdefault(proc_info.ppid, []).append(pid)

        firex_ids_by_pid = {
            fp.proc_info.pid: fp.firex_id for fp in firex_procs_by_os_procs.values()
        }

        def add(proc: psutil.Process, proc_info: _OsProcInfo, firex_id: str) -> None:
            firex_proc = FireXProc.create_from_proc_info(
                proc_info,
                proc,
                "localhost",
                firex_id=firex_id,
            )
            if self._matches(firex_proc):
                firex_procs_by_os_procs[proc] = firex_proc
                firex_ids_by_pid[proc_info.pid] = firex_id

        submits = [
            (proc, proc_info)
            for proc, proc_info in proc_infos_by_pid.values()
            if proc_info.pid not in firex_ids_by_pid and proc_info.is_firex_submit_cmd()
        ]
        for proc, proc_info in submits:
            child_infos = [
                proc_infos_by_pid[child_pid]
                for child_pid in children_by_ppid.get(proc_info.pid, [])
            ]
            # a submit's own children were started with the FireX ID of the run this
            # submit owns, which is the one to attribute the submit to -- any FireX ID
            # in the submit's own environ came from an outer run that launched it.
            child_firex_ids = {
                firex_ids_by_pid[child_info.pid]
                for _, child_info in child_infos
                if child_info.pid in firex_ids_by_pid
            }
            if len(child_firex_ids) == 1:
                add(proc, proc_info, child_firex_ids.pop())
            elif child_firex_ids:
                logger.warning(
                    f"Not identifying submit proc {proc_info.pid} from its children: they "
                    f"disagree on the FireX ID ({', '.join(sorted(child_firex_ids))})."
                )
            elif any(
                child_info.is_defunct()
                and child_info.get_firex_proc_type().is_infra_process(
                    tracking_service=True
                )
                for _, child_info in child_infos
            ):
                # Every tracking service it started is defunct, so there is no run left
                # to name it. Give it an ID of its own so leak detection can still see
                # it, and so its defunct children land under the same ID below.
                add(
                    proc,
                    proc_info,
                    _create_unknown_firex_id(proc_info.get_age(), proc_info.pid),
                )

        for pid, (proc, proc_info) in proc_infos_by_pid.items():
            if (
                pid not in firex_ids_by_pid
                and proc_info.is_defunct()
                # only a known FireX proc type, never a defunct proc that merely happens
                # to be a child of one (e.g. a tool a run shelled out to).
                and proc_info.get_firex_proc_type().is_infra_process()
                and proc_info.ppid in firex_ids_by_pid
            ):
                add(proc, proc_info, firex_ids_by_pid[proc_info.ppid])

    def query_firex_procs(self, hostname=None) -> list[FireXProc]:
        firex_procs: list[FireXProc]
        if hostname is None:
            firex_procs = self._query_cur_host_procs()
        else:
            firex_procs = self.query_remote_host_procs(hostname)

        return firex_procs

    def query_procs_per_run(self, hostname=None) -> dict[str, _SingleRunFireXProcs]:
        if hostname is not None and hostname == socket.gethostname():
            hostname = None  # avoid ssh to localhost

        matching_firex_procs = self.query_firex_procs(hostname=hostname)

        firex_ids_to_procs = {}
        for firex_proc in matching_firex_procs:
            if firex_proc.firex_id not in firex_ids_to_procs:
                firex_ids_to_procs[firex_proc.firex_id] = []
            firex_ids_to_procs[firex_proc.firex_id].append(firex_proc)

        return {
            fid: _SingleRunFireXProcs(fid, fid_procs)
            for fid, fid_procs in firex_ids_to_procs.items()
        }


class CommonFireXProcQueries:
    @staticmethod
    def query_single_run_procs(
        firex_id,
        proc_types=None,
        hostname=None,
        query_recursive_children=None,
    ) -> _SingleRunFireXProcs | None:
        proc_types = (
            [
                t
                for t in FireXProcType
                # None means all.
            ]
            if proc_types is None
            else proc_types
        )

        return (
            FireXProcQuery(
                proc_types=proc_types,
                query_min_age=None,
                query_username=None,
                firex_id=firex_id,
                query_recursive_children=query_recursive_children,
            )
            .query_procs_per_run(hostname)
            .get(firex_id)
        )

    @staticmethod
    def query_single_run_firex_procs(
        firex_id,
        proc_types=None,
        hostname=None,
    ) -> list["FireXProc"]:
        single_run = CommonFireXProcQueries.query_single_run_procs(
            firex_id,
            proc_types,
            hostname,
        )
        return single_run.firex_procs if single_run else []

    @staticmethod
    def get_broker_proc(firex_id: str, hostname=None) -> Optional["FireXProc"]:
        firex_procs = CommonFireXProcQueries.query_single_run_firex_procs(
            firex_id,
            [FireXProcType.REDIS_SERVER],
            hostname=hostname,
        )
        return firex_procs[0] if firex_procs else None

    @staticmethod
    def get_cur_host_broker_proc(firex_id: str) -> psutil.Process | None:
        broker_proc = CommonFireXProcQueries.get_broker_proc(firex_id)
        return broker_proc.os_proc if broker_proc else None

    @staticmethod
    def find_proc_firex_ids(
        proc_types=None,  # None means query all firex proc types
        query_username=None,  # None means query procs from all users
        hostname=None,
        query_max_age=None,
    ) -> list[str]:
        if proc_types is None:
            proc_types = [t for t in FireXProcType]
        return list(
            FireXProcQuery(
                proc_types=proc_types,
                query_username=query_username,
                query_max_age=query_max_age,
            )
            .query_procs_per_run(hostname)
            .keys()
        )

    @staticmethod
    def find_redis_procs_firex_ids(hostname=None, query_min_age=None) -> list[str]:
        return list(
            FireXProcQuery(
                proc_types=[FireXProcType.REDIS_SERVER],
                query_min_age=query_min_age,
            )
            .query_procs_per_run(hostname)
            .keys()
        )

    @staticmethod
    def is_firex_id_redis_proc_alive(firex_id, hostname) -> bool:
        return bool(
            FireXProcQuery(
                firex_id=firex_id,
                proc_types=[FireXProcType.REDIS_SERVER],
            ).query_procs_per_run(hostname)
        )

    @staticmethod
    def find_cur_user_cur_host_procs(
        query_recursive_children: bool | None,
    ) -> dict[str, list["FireXProc"]]:
        fids_to_procs = FireXProcQuery(
            query_username=getuser(),
            # get_leaked_reason narrows by age,
            # query with no time restriction here.
            query_min_age=None,
            query_recursive_children=query_recursive_children,
        ).query_procs_per_run(hostname=None)
        return {k: v.firex_procs for k, v in fids_to_procs.items()}

    @staticmethod
    def all_cur_user_cur_host_fx_procs_from_celery_worker(
        query_firex_id: str,
        query_worker_id: str,
    ) -> bool:
        cur_host_and_user_procs: dict[str, list[FireXProc]] = (
            CommonFireXProcQueries.find_cur_user_cur_host_procs(
                query_recursive_children=False,
            )
        )
        return bool(
            len(cur_host_and_user_procs) == 1
            and cur_host_and_user_procs.get(query_firex_id)
            and all(
                fxp.celery_destination == query_worker_id
                for fxp in cur_host_and_user_procs[query_firex_id]
            )
        )


def _log_to_module_logger(level: int, msg: str) -> None:
    logger.log(level, msg)


def _proc_desc(firex_proc: FireXProc) -> str:
    """Something legible for a proc whose cmdline may be gone (defunct procs lose it)."""
    return (
        " ".join(firex_proc.proc_info.cmdline)
        or f"[{firex_proc.proc_info.name}] <defunct>"
    )


def force_kill_firex_procs(
    firex_procs: list[FireXProc],
    log: KillLogFn = _log_to_module_logger,
) -> list[FireXProc]:
    """Force kills the supplied processes, returning those confirmed to be gone."""
    kill_attempted_os_to_firex_procs: dict[psutil.Process, FireXProc] = {}
    cur_proc = psutil.Process()
    cur_proc_and_ancestors = cur_proc.parents() + [cur_proc]

    # A defunct proc has already exited: signalling it does nothing, and waiting on one
    # that isn't this process' own child blocks for the full timeout only to report it
    # still alive. It goes away when whatever owes it a wait() is killed below, so keep
    # it out of the kill and the wait, and check for it afterwards instead.
    defunct_firex_procs = [fp for fp in firex_procs if fp.is_defunct()]

    for firex_proc in firex_procs:
        if (
            firex_proc.os_proc is None
            or firex_proc.is_defunct()
            or firex_proc.os_proc in cur_proc_and_ancestors
        ):
            pass  # ignored, don't want to kill the current process.
        else:
            kill_sent = firex_proc.kill()
            if kill_sent:
                kill_attempted_os_to_firex_procs[firex_proc.os_proc] = firex_proc

    gone = psutil.wait_procs(
        procs=kill_attempted_os_to_firex_procs.keys(),
        timeout=10,  # force kill shouldn't take long
    )[0]

    # Once whatever owed them a wait() is gone, init inherits them and reaps them, so
    # this returns as soon as that has happened rather than costing the full timeout.
    reaped = psutil.wait_procs(
        procs=[fp.os_proc for fp in defunct_firex_procs if fp.os_proc],
        timeout=3,
    )[0]

    killed_firex_procs = []
    for os_proc, firex_proc in kill_attempted_os_to_firex_procs.items():
        if os_proc in gone:
            killed_firex_procs.append(firex_proc)
            log(logging.WARNING, f"Force killed proc: {_proc_desc(firex_proc)}")
        else:
            logger.warning(f"Failed to kill: {_proc_desc(firex_proc)}")

    for firex_proc in defunct_firex_procs:
        if firex_proc.os_proc in reaped:
            killed_firex_procs.append(firex_proc)
            log(logging.WARNING, f"Reaped defunct proc: {_proc_desc(firex_proc)}")
        else:
            logger.warning(f"Defunct proc was not reaped: {_proc_desc(firex_proc)}")

    if not killed_firex_procs:
        firex_ids = ", ".join({fos.firex_id for fos in firex_procs})
        logger.warning(f"Killed no FireX processes for {firex_ids}")

    return killed_firex_procs
