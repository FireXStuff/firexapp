from firexapp.submit.console import setup_console_logging
from firexapp.submit.uid import Uid
from logging import INFO, DEBUG, WARNING
import os
import re
import subprocess
import psutil
from socket import gethostname
from typing import Optional

from firexapp.broker_manager.broker_factory import BrokerFactory
from firexapp.common import poll_until_file_not_empty, poll_until_dir_empty
from firexapp.plugins import PLUGINS_ENV_NAME, cdl2list
from collections.abc import Iterable
from firexapp.common import qualify_firex_bin


logger = setup_console_logging(__name__)


class CeleryWorkerStartFailed(Exception):
    pass


class CeleryManager:

    def __init__(
        self,
        logs_dir: str,
        plugins=None,
        worker_log_level='debug',
        cap_concurrency=None,
        app='firexapp.engine',
        env=None,
        broker=None,
    ):

        if not broker:
            self.broker = BrokerFactory.get_broker_url(assert_if_not_set=True)

        self.hostname = gethostname()
        self.plugins = plugins
        self.logs_dir = logs_dir
        self.worker_log_level = worker_log_level
        self.cap_concurrency = cap_concurrency
        self.app = app

        self.env = os.environ | {
            'CELERY_RDBSIG': '1',
            PLUGINS_ENV_NAME: ",".join(cdl2list(plugins)),
        }
        if env:
            self.update_env(env)

        self.pid_files: dict[str, str] = dict()
        self._celery_logs_dir = None
        self._celery_pids_dir = None
        self._workers_logs_dir = None

    @classmethod
    def log(cls, msg, header=None, level=DEBUG):
        if header is None:
            header = cls.__name__
        if header:
            msg = '[%s] %s' % (header, msg)
        logger.log(level, msg)

    def update_env(self, env):
        assert isinstance(env, dict), 'env needs to be a dictionary'
        self.env.update({k: str(v) for k, v in env.items()})

    @classmethod
    def get_celery_logs_dir(cls, logs_dir: str) -> str:
        return os.path.join(logs_dir, Uid.debug_dirname, 'celery')

    @classmethod
    def get_celery_pids_dir(cls, logs_dir):
        return os.path.join(cls.get_celery_logs_dir(logs_dir), 'pids')

    @staticmethod
    def get_worker_logs_dir(logs_dir: str) -> str:
        return os.path.join(logs_dir, 'microservice_logs')

    @property
    def celery_logs_dir(self):
        if not self._celery_logs_dir:
            _celery_logs_dir = self.get_celery_logs_dir(self.logs_dir)
            os.makedirs(_celery_logs_dir, exist_ok=True)
            self._celery_logs_dir = _celery_logs_dir
        return self._celery_logs_dir

    @property
    def celery_pids_dir(self):
        if not self._celery_pids_dir:
            _celery_pids_dir = self.get_celery_pids_dir(self.logs_dir)
            os.makedirs(_celery_pids_dir, exist_ok=True)
            self._celery_pids_dir = _celery_pids_dir
        return self._celery_pids_dir

    @property
    def workers_logs_dir(self):
        if not self._workers_logs_dir:
            _workers_logs_dir = self.get_worker_logs_dir(self.logs_dir)
            os.makedirs(_workers_logs_dir, exist_ok=True)
            self._workers_logs_dir = _workers_logs_dir
        return self._workers_logs_dir

    @classmethod
    def get_worker_log_file(cls, logs_dir, worker_and_host):
        return cls.__get_worker_log_file(cls.get_worker_logs_dir(logs_dir), worker_and_host)

    def _get_worker_log_file(self, workername):
        queue_and_worker = self.get_worker_and_host(workername, self.hostname)
        return self.__get_worker_log_file(self.workers_logs_dir, queue_and_worker)

    @staticmethod
    def __get_worker_log_file(worker_logs_dir, worker_and_host):
        return os.path.join(worker_logs_dir, '%s.html' % worker_and_host)

    @classmethod
    def get_pid_file(cls, logs_dir, workername, hostname=gethostname()):
        worker_and_host = cls.get_worker_and_host(workername, hostname)
        return cls.__get_pid_file(cls.get_celery_pids_dir(logs_dir), worker_and_host)

    def _get_pid_file(self, workername):
        worker_and_host = self.get_worker_and_host(workername, self.hostname)
        return self.__get_pid_file(self.celery_pids_dir, worker_and_host)

    @staticmethod
    def __get_pid_file(pids_logs_dir, worker_and_host):
        return os.path.join(pids_logs_dir, '%s.pid' % worker_and_host)

    def _get_stdout_file(self, workername):
        return os.path.join(self.celery_logs_dir, '%s@%s.stdout.txt' % (workername, self.hostname))

    @staticmethod
    def get_worker_and_host(workername, hostname):
        return '%s@%s' % (workername, hostname)

    @classmethod
    def get_pid_from_file(cls, pid_file):
        try:
            with open(pid_file) as f:
                pid = f.read().strip()
        except FileNotFoundError:
            cls.log(f'No pid file found in {pid_file}', level=WARNING)
            raise
        else:
            if pid:
                return int(pid)
            else:
                raise AssertionError('no pid')

    @classmethod
    def get_pid(cls, logs_dir, workername, hostname=gethostname()):
        pid_file = cls.get_pid_file(logs_dir, workername, hostname)
        return cls.get_pid_from_file(pid_file)

    @classmethod
    def get_worker_pids(cls, logs_dir, hostname, workernames):
        hostname = gethostname() if hostname == 'localhost' else hostname
        pids = []
        for workername in workernames:
            try:
                pid = cls.get_pid(logs_dir, workername, hostname)
            except Exception as e:
                cls.log(e)
            else:
                pids.append(pid)
        return pids

    @staticmethod
    def cap_cpu_count(count, cap_concurrency):
        return min(count, cap_concurrency) if cap_concurrency else count

    def extract_errors_from_celery_logs(self, celery_log_file, max_errors=20):
        err_list = None
        try:
            with open(celery_log_file, encoding='ascii', errors='ignore') as f:
                logs = f.read()
                err_list = re.findall(r'^\S*Error: .*$', logs, re.MULTILINE)
                if err_list:
                    err_list = err_list[0:max_errors]
        except FileNotFoundError:
            pass

        return err_list

    def wait_until_active(self, pid_file, stdout_file, workername, timeout=15*60):
        extra_err_info = ''
        try:
            poll_until_file_not_empty(pid_file, timeout=timeout)
        except AssertionError:
            err_list = self.extract_errors_from_celery_logs(stdout_file)
            if err_list:
                extra_err_info += '\nFound the following errors:\n' + '\n'.join(err_list)

            extra_err_info += '\nAttempting to delete the invocation pids'
            deleted_pids = subprocess.run(
                ['/bin/pkill', '-e', '-f', pid_file],
                capture_output=True,
                text=True,
            )
            if deleted_pids.stdout:
                extra_err_info += f'\nstdout: {deleted_pids.stdout}'
            if deleted_pids.stderr:
                extra_err_info += f'\nstderr: {deleted_pids.stderr}'

            raise CeleryWorkerStartFailed(
                f'The worker {workername}@{self.hostname} did not come up after'
                f' {timeout} seconds.\n'
                f'Please look into {stdout_file!r} for details.'
                f'{extra_err_info}'
            )
        pid = self.get_pid_from_file(pid_file)
        self.log(f'pid {pid} became active')

    def start(
        self,
        workername: str,
        queues=None,
        wait=True,
        timeout=15*60,
        concurrency=None,
        worker_log_level=None,
        cap_concurrency=None,
        cwd=None,
        soft_time_limit=None,
        autoscale: Optional[tuple]=None,
        detach: bool=True,
    ):

        # Override defaults if applicable
        worker_log_level = worker_log_level if worker_log_level else self.worker_log_level
        cap_concurrency = cap_concurrency if cap_concurrency else self.cap_concurrency

        stdout_file = self._get_stdout_file(workername)
        log_file = self._get_worker_log_file(workername)
        pid_file = self._get_pid_file(workername)
        self.pid_files[workername] = pid_file

        cmd = f'{qualify_firex_bin("celery")} ' \
              f'--app={self.app} worker ' \
              f'--hostname={workername}@%h ' \
              f'--loglevel={worker_log_level} ' \
              f'--logfile={log_file} ' \
              f'--pidfile={pid_file} ' \
              f'--events ' \
              f'--without-gossip ' \
              f'--without-heartbeat ' \
              f'--without-mingle ' \
              f'-Ofair'
        if queues:
            cmd += ' --queues=%s' % queues

        if concurrency and autoscale:
            raise AssertionError('You can either provide a value of concurrency or autoscale, but not both')

        if concurrency:
            cmd += ' --concurrency=%d' % self.cap_cpu_count(concurrency, cap_concurrency)
        elif autoscale:
            assert isinstance(autoscale, Iterable), 'autoscale should be a tuple of (min, max)'
            assert len(autoscale) == 2, 'autoscale should be a tuple of two elements (min, max)'
            autoscale_v1, autoscale_v2 = autoscale
            autoscale_min = self.cap_cpu_count(
                min(autoscale_v1, autoscale_v2),
                cap_concurrency)
            autoscale_max = self.cap_cpu_count(
                max(autoscale_v1, autoscale_v2),
                cap_concurrency)
            cmd += f' --autoscale={autoscale_max},{autoscale_min}'
        if soft_time_limit:
            cmd += f' --soft-time-limit={soft_time_limit}'

        if detach:
            cmd += ' &'

        self.log('Starting %s on %s...' % (workername, self.hostname))
        self.log(cmd)

        if cwd:
            self.log(f'cwd={cwd}')

        with open(stdout_file, 'ab') as fp:
            subprocess.check_call(
                cmd,
                shell=True,
                stdout=fp,
                stderr=subprocess.STDOUT,
                env=self.env,
                cwd=cwd,
            )

        if detach and wait:
            self.wait_until_active(
                pid_file=pid_file,
                timeout=timeout,
                stdout_file=stdout_file,
                workername=workername,
            )

    @staticmethod
    def find_procs(pid_file):
        return find_procs(
            'celery',
            cmdline_contains=f'--pidfile={pid_file}',
        )

    def find_all_procs(self):
        procs = []
        for pid_file in os.listdir(self.celery_pids_dir):
            procs += self.find_procs(os.path.join(self.celery_pids_dir, pid_file))
        return procs

    def kill_all_forked(self, pid_file):
        for proc in self.find_procs(pid_file):
            self.log('Killing  pid %d' % proc.pid, level=INFO)
            try:
                proc.kill()
            except Exception:
                self.log('Failed to kill pid %d' % proc.pid, level=WARNING)

    @classmethod
    def terminate(cls, pid, timeout=60):
        cls.log(f'Terminating pid {pid}', level=INFO)
        p = psutil.Process(pid)
        p.terminate()
        p.wait(timeout=timeout)

    def shutdown(self, timeout=60):
        if self.pid_files:
            name_to_pid_file = self.pid_files
        else:
            # self.pid_files is only populated when starting celery, so if this manager didn't start the celery
            # instance being operated on, fallback to the pid directory.
            name_to_pid_file = {
                pf: os.path.join(self.celery_pids_dir, pf)
                for pf in os.listdir(self.celery_pids_dir)
            }

        for name, pid_file in name_to_pid_file.items():
            self.log(f'Attempting shutdown of {name}')
            try:
                pid = self.get_pid_from_file(pid_file)
            except Exception as e:
                self.log(e)
            else:
                try:
                    self.terminate(pid, timeout=timeout)
                except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                    self.kill_all_forked(pid_file)
                except Exception as e:
                    self.log(e)

    def wait_for_shutdown(self, timeout=15):
        return poll_until_dir_empty(
            self.celery_pids_dir,
            timeout=timeout,
        )


def find_procs(name, cmdline_regex=None, cmdline_contains=None):
    matching_procs = []
    if cmdline_regex:
        cmdline_regex = re.compile(cmdline_regex)
    else:
        cmdline_regex = None
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['name', 'cmdline', 'pid'])
        except psutil.NoSuchProcess:
            pass
        else:
            if proc_matches(pinfo, name, cmdline_regex, cmdline_contains):
                matching_procs.append(proc)

    return matching_procs