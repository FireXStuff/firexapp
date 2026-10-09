import os
import pathlib
import re
import shlex
import subprocess
import time
from collections.abc import Iterable
from logging import DEBUG, INFO, WARNING
from socket import gethostname

import psutil

import firexapp.firex_subprocess
from firexapp.common import (
    FIREX_BIN_DIR_ENV,
    poll_until_dir_empty,
    qualify_firex_bin,
)
from firexapp.engine.default_celery_config import FxEnvVars
from firexapp.plugins import FxPluginRegistry
from firexapp.submit.console import setup_console_logging
from firexapp.submit.uid import Uid
from firexkit.firex_worker import FxWorkerHostName, FxWorkerId, FxWorkerName

logger = setup_console_logging(__name__)

_PID_FILE_SUFFIX = ".pid"

# How often a worker that hasn't written its pid file yet is checked for still
# existing. Each check walks the whole process table, so it is deliberately much
# coarser than the pid file poll it rides along with.
_LIVENESS_POLL_INTERVAL_SECS = 2.0


class CeleryWorkerStartFailed(Exception):
    pass


class CeleryVenvMismatchError(Exception):
    pass


def get_venv_bin_dir(virtual_env: str) -> str:
    return os.path.normpath(os.path.join(virtual_env, "bin"))


class CeleryManager:
    def __init__(
        self,
        logs_dir: str,
        fx_env: FxEnvVars,
        plugins: None | str | list[str] = None,
        env: dict[str, str] | None = None,
    ):
        self.plugins = plugins
        self.logs_dir = logs_dir

        fx_env = fx_env.model_copy(
            update={
                "firex_plugins": ",".join(
                    FxPluginRegistry.resolve_plugin_paths(plugins)
                ),
            },
        )
        self.env = (
            os.environ
            | fx_env.model_dump()
            | {
                "CELERY_RDBSIG": "1",
                "FIREX_START_CELERY_WORKER": "True",
                # Forces glibc to share far fewer arenas across threads, trading a
                # little lock contention for substantially less fragmentation on
                # bursty I/O-bound workloads like Regress/MBnR
                "MALLOC_ARENA_MAX": "2",
            }
        )
        if env:
            self.update_env(env)

        self.pid_files: dict[FxWorkerId, str] = {}

        self._celery_logs_dir = None
        self._celery_pids_dir = None
        self._workers_logs_dir = None

    @classmethod
    def is_current_env_fx_celery_worker(cls) -> bool:
        return os.environ.get("FIREX_START_CELERY_WORKER") == "True"

    @classmethod
    def unset_start_fx_celery_worker_env(cls):
        os.environ.pop("FIREX_START_CELERY_WORKER", None)

    @classmethod
    def log(cls, msg, header=None, level=DEBUG):
        if header is None:
            header = cls.__name__
        if header:
            msg = f"[{header}] {msg}"
        logger.log(level, msg)

    def update_env(self, env: dict[str, str]):
        assert isinstance(env, dict), "env needs to be a dictionary"
        self.env.update({k: str(v) for k, v in env.items()})

    @classmethod
    def _get_celery_pids_dir(cls, logs_dir: str) -> str:
        return os.path.join(logs_dir, Uid.debug_dirname, "celery", "pids")

    @staticmethod
    def get_worker_logs_dir(logs_dir: str) -> str:
        return os.path.join(logs_dir, "microservice_logs")

    @property
    def celery_pids_dir(self):
        if not self._celery_pids_dir:
            _celery_pids_dir = self._get_celery_pids_dir(self.logs_dir)
            os.makedirs(_celery_pids_dir, exist_ok=True)
            self._celery_pids_dir = _celery_pids_dir
        return self._celery_pids_dir

    @staticmethod
    def _get_pid_file(pids_logs_dir: str, worker_id: FxWorkerId) -> str:
        return os.path.join(
            pids_logs_dir,
            f"{worker_id}{_PID_FILE_SUFFIX}",
        )

    @staticmethod
    def _find_pid_files_by_name(
        pids_logs_dir: str,
        worker_name: FxWorkerHostName,
    ) -> list[str]:
        """The pid files of all workers named 'worker_name', least recently written first."""
        pid_files = [
            os.path.join(pids_logs_dir, f)
            for f, worker_id in _get_pid_file_worker_ids(pids_logs_dir).items()
            if worker_id and worker_id.as_host_worker_name() == worker_name
        ]
        return sorted(pid_files, key=os.path.getmtime)

    @classmethod
    def get_celery_pid(
        cls,
        logs_dir: str,
        worker_and_host: str | FxWorkerHostName | FxWorkerId,
    ) -> int:
        """Get the pid of the worker with the supplied ID, or name.

        Since a name, unlike an ID, can be re-used by successive workers, a name
        matching several pid files resolves to the most recently written one.
        """
        pids_logs_dir = cls._get_celery_pids_dir(logs_dir)
        if isinstance(worker_and_host, FxWorkerId):
            pid_file = cls._get_pid_file(pids_logs_dir, worker_and_host)
        else:
            if isinstance(worker_and_host, str):
                worker_and_host = FxWorkerHostName.fx_worker_host_name_from_str(
                    worker_and_host,
                )
            pid_files = cls._find_pid_files_by_name(
                pids_logs_dir,
                worker_and_host,
            )
            if not pid_files:
                logger.warning(
                    f"No pid file found for {worker_and_host} in {pids_logs_dir}"
                )
                raise FileNotFoundError(f"No pid file for worker {worker_and_host}")
            pid_file = pid_files[-1]
        return _get_pid_from_file(pid_file)

    @staticmethod
    def cap_cpu_count(count, cap_concurrency):
        return min(count, cap_concurrency) if cap_concurrency else count

    def _assert_worker_venv_consistent(self):
        """
        Refuse to start a worker whose bin dir and venv disagree.

        qualify_firex_bin resolves celery to an absolute path under
        firex_bin_dir, so that variable alone decides which install's
        interpreter the worker runs, while the worker's inherited PYTHONPATH
        still resolves modules out of VIRTUAL_ENV. A mismatch therefore runs one
        install's celery against the other's modules, which only surfaces once
        the worker boots, as an ImportError naming neither the two installs nor
        the run that mixed them.

        firex_bin_dir is inherited, so a cascading invocation (a run that builds
        a new install and then submits into it) is how the two come apart.
        """
        virtual_env = self.env.get("VIRTUAL_ENV")
        firex_bin_dir = self.env.get(FIREX_BIN_DIR_ENV)
        if not virtual_env or not firex_bin_dir:
            return

        expected_bin_dir = get_venv_bin_dir(virtual_env)
        if os.path.normpath(firex_bin_dir) != expected_bin_dir:
            raise CeleryVenvMismatchError(
                f"Refusing to start a celery worker from a different install"
                f" than this run: {FIREX_BIN_DIR_ENV}={firex_bin_dir} but"
                f" VIRTUAL_ENV={virtual_env} (expected {expected_bin_dir})."
                f" Workers would run {firex_bin_dir}/celery against"
                f" {virtual_env} modules."
            )

    def start_celery_worker(
        self,
        workername: str,
        queues: str,
        wait_celery_active=True,
        timeout=15 * 60,
        concurrency=None,
        cap_concurrency=None,
        cwd=None,
        soft_time_limit=None,
        autoscale: tuple | None = None,
        celery_cmd_log_level=DEBUG,
        app="firexapp.engine.celery:app",
        extra_celery_args: list[str] | None = None,
    ) -> FxWorkerId:
        self._assert_worker_venv_consistent()

        # Celery only ever knows this worker by its name, but files belonging to
        # this particular worker instance are identified by its ID, so that a
        # worker re-using the name doesn't clobber them.
        worker_id = (
            FxWorkerName.fx_worker_name_from_str(workername)
            .as_host_worker(gethostname())
            .as_worker_id()
        )
        celery_worker_name = worker_id.as_host_worker_name()

        pid_path = pathlib.Path(self._get_pid_file(self.celery_pids_dir, worker_id))
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        self.pid_files[worker_id] = str(pid_path)

        cel_worker_logfile = pathlib.Path(
            self.get_worker_logs_dir(self.logs_dir),
            # deliberately named after the worker, not the ID: this log file is
            # shared by all the workers that have had this name on this host.
            f"{celery_worker_name}.html",
        )
        tasks_logs_dir = cel_worker_logfile.parent
        tasks_logs_dir.mkdir(parents=True, exist_ok=True)

        # A list, not a string: every path in here comes from a caller-supplied
        # logs dir, and a shell splitting one of them on a space would surface
        # only as a worker that never writes its pid file.
        cmd = [
            qualify_firex_bin("celery"),
            f"--app={app}",
            "worker",
            f"--hostname={celery_worker_name}",
            "--loglevel=debug",
            f"--logfile={cel_worker_logfile}",
            f"--pidfile={pid_path}",
            "--events",
            "--without-gossip",
            "--without-heartbeat",
            "--without-mingle",
            "-Ofair",
        ]
        if queues:
            cmd.append(f"--queues={queues}")

        if concurrency and autoscale:
            raise AssertionError(
                "You can either provide a value of concurrency or autoscale, but not both"
            )

        if concurrency:
            cmd.append(
                f"--concurrency={self.cap_cpu_count(concurrency, cap_concurrency)}"
            )
        elif autoscale:
            assert isinstance(autoscale, Iterable), (
                "autoscale should be a tuple of (min, max)"
            )
            assert len(autoscale) == 2, (
                "autoscale should be a tuple of two elements (min, max)"
            )
            autoscale_v1, autoscale_v2 = autoscale
            autoscale_min = self.cap_cpu_count(
                min(autoscale_v1, autoscale_v2), cap_concurrency
            )
            autoscale_max = self.cap_cpu_count(
                max(autoscale_v1, autoscale_v2), cap_concurrency
            )
            cmd.append(f"--autoscale={autoscale_max},{autoscale_min}")

        if soft_time_limit:
            cmd.append(f"--soft-time-limit={soft_time_limit}")

        # Deliberately appended, i.e. after the 'worker' subcommand: an option
        # an app contributes via user_options['preload'] is attached to each
        # subcommand and not to the top-level group (celery/bin/celery.py), so
        # it is only parseable here. Appending also keeps this clear of the
        # 'worker' element itself, wherever it ends up in cmd.
        cmd.extend(extra_celery_args or [])

        self.log(f"Starting {worker_id}...")
        stdout_file = os.path.join(pid_path.parent.parent, f"{worker_id}.stdout.txt")
        try:
            firexapp.firex_subprocess.check_output(
                # shlex.join, so that a logs dir with a space in it reaches
                # celery as one argument. The shell is here only for the
                # trailing '&'; it returns as soon as it has forked.
                shlex.join(cmd) + " &",
                shell=True,
                file=stdout_file,
                env=self.env,
                cwd=cwd,
                log_level=celery_cmd_log_level,
                remove_firex_pythonpath=False,
                # The only thing putting the worker in a session of its own:
                # celery isn't daemonising, and a backgrounded job of a
                # non-interactive shell otherwise keeps the submitting shell's
                # process group and session, so a Ctrl+C or pgid-wide kill
                # aimed at that shell would reach the worker long after the
                # shell itself was gone.
                start_new_session=True,
            )
        except (firexapp.firex_subprocess.CommandFailed, OSError) as e:
            # Only covers failing to get the shell started at all -- a cwd that
            # doesn't exist, say. The trailing '&' means the shell reports its
            # own exit status, which is 0 however celery fares, so every
            # failure of the worker itself is left to _wait_until_active.
            raise CeleryWorkerStartFailed(
                f"The shell launching {worker_id} failed: {e}\n"
                f"Please look into {stdout_file!r} for details."
                f"{_describe_errors_in_file(stdout_file)}"
            ) from e

        if wait_celery_active:
            _wait_until_active(
                pid_file=str(pid_path),
                timeout=timeout,
                stdout_file=stdout_file,
                worker_logfile=str(cel_worker_logfile),
                worker_id=worker_id,
            )

        return worker_id

    def find_all_procs(self):
        procs = []
        for pid_file in os.listdir(self.celery_pids_dir):
            procs += _find_all_celery_procs_by_cmdline_pidfile_arg(
                pid_file=os.path.join(self.celery_pids_dir, pid_file)
            )
        return procs

    def shutdown(self, timeout=60.0):
        if self.pid_files:
            worker_id_to_pid_file = self.pid_files
        else:
            # self.pid_files is only populated when starting celery, so if this manager didn't start the celery
            # instance being operated on, fallback to the pid directory.
            worker_id_to_pid_file = {
                # fallback to the filename for pid files that aren't named after a worker ID.
                worker_id or pid_filename: os.path.join(
                    self.celery_pids_dir, pid_filename
                )
                for pid_filename, worker_id in _get_pid_file_worker_ids(
                    self.celery_pids_dir,
                ).items()
            }

        for worker_id, pid_file in worker_id_to_pid_file.items():
            self.log(f"Attempting shutdown of {worker_id}")
            try:
                pid = _get_pid_from_file(pid_file)
            except (AssertionError, OSError, ValueError) as e:
                self.log(e)
            else:
                try:
                    self.log(f"Terminating pid {pid}", level=INFO)
                    p = psutil.Process(pid)
                    p.terminate()
                    p.wait(timeout=timeout)
                except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                    for proc in _find_all_celery_procs_by_cmdline_pidfile_arg(pid_file):
                        self.log(f"Killing  pid {proc.pid}", level=INFO)
                        try:
                            proc.kill()
                        except psutil.Error:
                            self.log(f"Failed to kill pid {proc.pid}", level=WARNING)
                except psutil.Error as e:
                    self.log(e)

    def wait_for_shutdown(self, timeout=15):
        return poll_until_dir_empty(
            self.celery_pids_dir,
            timeout=timeout,
        )

    def wait_for_worker_shutdown(
        self,
        worker_id: FxWorkerId,
        timeout: float | None = None,
    ) -> bool:
        """Wait for a worker to exit. False if it is still up at 'timeout'.

        Waits on the processes rather than on the pid file: celery unlinks its
        pid file from an atexit handler (celery.platforms.create_pidlock), so a
        worker that is killed outright -- by the OOM killer, most often --
        leaves the file behind and would never be seen to stop. A 'timeout' of
        None waits indefinitely, which is only safe for that reason.
        """
        pid_file = self.pid_files.get(worker_id) or self._get_pid_file(
            self.celery_pids_dir,
            worker_id,
        )
        # One process table walk, then a wait per process: the pool children
        # share the worker's command line, so this covers them too.
        _, alive = psutil.wait_procs(
            _find_all_celery_procs_by_cmdline_pidfile_arg(pid_file),
            timeout=timeout,
        )
        return not alive


def _get_pid_file_worker_ids(pids_logs_dir: str) -> dict[str, FxWorkerId | None]:
    """The ID of the worker owning each pid file in 'pids_logs_dir', by pid file name.

    The ID is None for any pid file not named after a worker ID, which can
    happen for pid files written by an older version of this module.
    """
    pid_file_worker_ids: dict[str, FxWorkerId | None] = {}
    for filename in os.listdir(pids_logs_dir):
        if filename.endswith(_PID_FILE_SUFFIX):
            try:
                worker_id = FxWorkerId.fx_worker_id_from_str(
                    filename[: -len(_PID_FILE_SUFFIX)],
                )
            except ValueError:
                worker_id = None
            pid_file_worker_ids[filename] = worker_id
    return pid_file_worker_ids


def _get_pid_from_file(pid_file: str) -> int:
    try:
        with open(pid_file) as f:
            pid = f.read().strip()
    except FileNotFoundError:
        logger.warning(f"No pid file found in {pid_file}")
        raise
    else:
        if pid:
            return int(pid)
        else:
            raise AssertionError("no pid")


def _wait_until_active(
    pid_file: str,
    stdout_file: str,
    worker_logfile: str,
    worker_id: FxWorkerId,
    timeout,
):
    """Wait for the backgrounded worker to write its pid file, or for it to die trying.

    The shell that backgrounded the worker reports its own exit status, which is
    0 however celery fares, so every failure of the worker itself lands here
    rather than in the launch. What the worker prints goes to 'stdout_file',
    which it inherits from that shell, until celery has logging up and switches
    to 'worker_logfile'. Neither is a signal on its own that a boot has failed:
    what that leaves here is a pid file that never appears, which is
    indistinguishable from a worker that is merely slow. Watching for the
    process is what tells those apart without waiting out the whole timeout.
    """
    timeout_time = time.time() + timeout
    # Also the grace period before the first check: the shell has exited by
    # now, but the worker it forked may not have been scheduled yet.
    next_liveness_poll = time.time() + _LIVENESS_POLL_INTERVAL_SECS
    while not (os.path.isfile(pid_file) and os.path.getsize(pid_file) > 0):
        now = time.time()
        if now >= next_liveness_poll:
            if not _find_all_celery_procs_by_cmdline_pidfile_arg(pid_file):
                raise CeleryWorkerStartFailed(
                    f"The worker {worker_id} exited before it became active.\n"
                    f"Please look into {stdout_file!r} and {worker_logfile!r}"
                    f" for details."
                    f"{_describe_errors_in_file(stdout_file)}"
                    f"{_describe_errors_in_file(worker_logfile)}"
                )
            next_liveness_poll = now + _LIVENESS_POLL_INTERVAL_SECS

        if now >= timeout_time:
            raise CeleryWorkerStartFailed(
                f"The worker {worker_id} did not come up after"
                f" {timeout} seconds.\n"
                f"Please look into {stdout_file!r} for details."
                f"{_describe_errors_in_file(stdout_file)}"
                f"{_kill_invocation_pids(pid_file)}"
            )
        time.sleep(0.1)

    pid = _get_pid_from_file(pid_file)
    logger.info(f"Celery pid {pid} became active for FireX worker: {worker_id}")


def _kill_invocation_pids(pid_file: str) -> str:
    deleted_pids = subprocess.run(
        ["/bin/pkill", "-e", "-f", pid_file],
        capture_output=True,
        check=False,
        text=True,
    )
    extra_err_info = "\nAttempting to delete the invocation pids"
    if deleted_pids.stdout:
        extra_err_info += f"\nstdout: {deleted_pids.stdout}"
    if deleted_pids.stderr:
        extra_err_info += f"\nstderr: {deleted_pids.stderr}"
    return extra_err_info


def _describe_errors_in_file(celery_log_file: str) -> str:
    err_list = _extract_errors_from_celery_logs(celery_log_file)
    if not err_list:
        return ""
    return "\nFound the following errors:\n" + "\n".join(err_list)


def _extract_errors_from_celery_logs(celery_log_file, max_errors=20):
    err_list = None
    try:
        with open(celery_log_file, encoding="ascii", errors="ignore") as f:
            logs = f.read()
            err_list = re.findall(r"^\S*Error: .*$", logs, re.MULTILINE)
            if err_list:
                err_list = err_list[0:max_errors]
    except FileNotFoundError:
        pass

    return err_list


def _find_all_celery_procs_by_cmdline_pidfile_arg(
    pid_file: str,
) -> list[psutil.Process]:
    """Every celery worker process invoked with '--pidfile=pid_file'.

    Matched on the command line alone, deliberately, rather than also on the
    process name: celery is launched through a console script, so what the
    process ends up named after depends on how that script was installed and on
    whether the pool children were forked or spawned. The pid file path is
    absolute and per-worker, so it identifies the worker on its own; requiring
    the 'worker' subcommand alongside it only rules out processes that merely
    mention the path.
    """
    cmdline_pidfile_part = f"--pidfile={pid_file}"
    matching_procs: list[psutil.Process] = []
    for proc in psutil.process_iter(["cmdline", "pid"]):
        try:
            cmdline = proc.info["cmdline"] or []
            if "worker" in cmdline and any(
                cmdline_pidfile_part in cmd_part for cmd_part in cmdline
            ):
                matching_procs.append(proc)
        except psutil.NoSuchProcess:
            pass

    return matching_procs
