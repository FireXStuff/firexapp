import os
import pathlib
import re
import subprocess
from collections.abc import Iterable
from logging import DEBUG, INFO, WARNING
from socket import gethostname

import psutil

import firexapp.firex_subprocess
from firexapp.common import (
    poll_until_dir_empty,
    poll_until_file_not_empty,
    qualify_firex_bin,
)
from firexapp.engine.default_celery_config import FxEnvVars
from firexapp.plugins import FxPluginRegistry
from firexapp.submit.console import setup_console_logging
from firexapp.submit.uid import Uid
from firexkit.firex_worker import FxWorkerHostName, FxWorkerId, FxWorkerName

logger = setup_console_logging(__name__)

_PID_FILE_SUFFIX = ".pid"


class CeleryWorkerStartFailed(Exception):
    pass


class CeleryManager:
    def __init__(
        self,
        logs_dir: str,
        fx_env: FxEnvVars,
        plugins: None | str | list[str] = None,
        app="firexapp.engine.celery:app",
        env=None,
    ):
        self.plugins = plugins
        self.logs_dir = logs_dir
        self.app = app

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

    def update_env(self, env):
        assert isinstance(env, dict), "env needs to be a dictionary"
        self.env.update({k: str(v) for k, v in env.items()})

    @classmethod
    def _get_celery_logs_dir(cls, logs_dir):
        return os.path.join(logs_dir, Uid.debug_dirname, "celery")

    @classmethod
    def _get_celery_pids_dir(cls, logs_dir):
        return os.path.join(cls._get_celery_logs_dir(logs_dir), "pids")

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
    def __get_pid_file(pids_logs_dir: str, worker_id: FxWorkerId) -> str:
        return os.path.join(pids_logs_dir, f"{worker_id}{_PID_FILE_SUFFIX}")

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
            pid_file = cls.__get_pid_file(pids_logs_dir, worker_and_host)
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

    def start_celery_worker(
        self,
        workername: str,
        queues=None,
        wait=True,
        timeout=15 * 60,
        concurrency=None,
        cap_concurrency=None,
        cwd=None,
        soft_time_limit=None,
        autoscale: tuple | None = None,
        detach: bool = True,
        celery_cmd_log_level=DEBUG,
    ) -> FxWorkerId:

        # Celery only ever knows this worker by its name, but files belonging to
        # this particular worker instance are identified by its ID, so that a
        # worker re-using the name doesn't clobber them.
        worker_id = (
            FxWorkerName.fx_worker_name_from_str(workername)
            .as_host_worker(gethostname())
            .as_worker_id()
        )
        celery_worker_name = worker_id.as_host_worker_name()

        pid_path = pathlib.Path(self.__get_pid_file(self.celery_pids_dir, worker_id))
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

        cmd = (
            f"{qualify_firex_bin('celery')} "
            f"--app={self.app} worker "
            f"--hostname={celery_worker_name} "
            f"--loglevel=debug "
            f"--logfile={cel_worker_logfile} "
            f"--pidfile={pid_path} "
            f"--events "
            f"--without-gossip "
            f"--without-heartbeat "
            f"--without-mingle "
            f"-Ofair"
        )
        if queues:
            cmd += f" --queues={queues}"

        if concurrency and autoscale:
            raise AssertionError(
                "You can either provide a value of concurrency or autoscale, but not both"
            )

        if concurrency:
            cmd += f" --concurrency={self.cap_cpu_count(concurrency, cap_concurrency)}"
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
            cmd += f" --autoscale={autoscale_max},{autoscale_min}"

        if soft_time_limit:
            cmd += f" --soft-time-limit={soft_time_limit}"

        if detach:
            cmd += " &"

        self.log(f"Starting {worker_id}...")
        stdout_file = os.path.join(pid_path.parent.parent, f"{worker_id}.stdout.txt")
        firexapp.firex_subprocess.check_output(
            cmd,
            shell=True,
            file=stdout_file,
            env=self.env,
            cwd=cwd,
            log_level=celery_cmd_log_level,
            remove_firex_pythonpath=False,
        )

        if detach and wait:
            _wait_until_active(
                pid_file=str(pid_path),
                timeout=timeout,
                stdout_file=stdout_file,
                worker_id=worker_id,
            )

        return worker_id

    @staticmethod
    def _find_procs(pid_file: str) -> list[psutil.Process]:
        return _find_procs(
            "celery",
            cmdline_contains=f"--pidfile={pid_file}",
        )

    def find_all_procs(self):
        procs = []
        for pid_file in os.listdir(self.celery_pids_dir):
            procs += self._find_procs(os.path.join(self.celery_pids_dir, pid_file))
        return procs

    def kill_all_forked(self, pid_file):
        for proc in self._find_procs(pid_file):
            self.log(f"Killing  pid {proc.pid}", level=INFO)
            try:
                proc.kill()
            except psutil.Error:
                self.log(f"Failed to kill pid {proc.pid}", level=WARNING)

    @classmethod
    def terminate(cls, pid, timeout=60):
        cls.log(f"Terminating pid {pid}", level=INFO)
        p = psutil.Process(pid)
        p.terminate()
        p.wait(timeout=timeout)

    def shutdown(self, timeout=60):
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
                    self.terminate(pid, timeout=timeout)
                except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                    self.kill_all_forked(pid_file)
                except psutil.Error as e:
                    self.log(e)

    def wait_for_shutdown(self, timeout=15):
        return poll_until_dir_empty(
            self.celery_pids_dir,
            timeout=timeout,
        )


def _get_pid_file_worker_ids(pids_logs_dir: str) -> dict[str, FxWorkerId | None]:
    """The ID of the worker owning each pid file in 'pids_logs_dir', by pid file name.

    The ID is None for any pid file not named after a worker ID, which can
    happen for pid files written by an older version of this module.
    """
    pid_file_worker_ids: dict[str, FxWorkerId | None] = {}
    for filename in os.listdir(pids_logs_dir):
        if not filename.endswith(_PID_FILE_SUFFIX):
            continue
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
    worker_id: FxWorkerId,
    timeout,
):
    extra_err_info = ""
    try:
        poll_until_file_not_empty(pid_file, timeout=timeout)
    except AssertionError:
        err_list = _extract_errors_from_celery_logs(stdout_file)
        if err_list:
            extra_err_info += "\nFound the following errors:\n" + "\n".join(err_list)

        deleted_pids = subprocess.run(
            ["/bin/pkill", "-e", "-f", pid_file],
            capture_output=True,
            check=False,
            text=True,
        )
        extra_err_info += "\nAttempting to delete the invocation pids"
        if deleted_pids.stdout:
            extra_err_info += f"\nstdout: {deleted_pids.stdout}"
        if deleted_pids.stderr:
            extra_err_info += f"\nstderr: {deleted_pids.stderr}"

        raise CeleryWorkerStartFailed(
            f"The worker {worker_id} did not come up after"
            f" {timeout} seconds.\n"
            f"Please look into {stdout_file!r} for details."
            f"{extra_err_info}"
        )
    pid = _get_pid_from_file(pid_file)
    logger.info(f"Celery pid {pid} became active")


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


def _find_procs(name, cmdline_contains=None) -> list[psutil.Process]:
    matching_procs = []
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=["name", "cmdline", "pid"])
        except psutil.NoSuchProcess:
            pass
        else:
            if _proc_matches(pinfo, name, cmdline_contains):
                matching_procs.append(proc)

    return matching_procs


def _proc_matches(proc_info, pname, cmdline_contains):
    if proc_info["name"] == pname:
        if cmdline_contains:
            return any(
                cmdline_contains in cmd_part
                for cmd_part in (proc_info["cmdline"] or [])
            )
        else:
            return True
    else:
        return False
