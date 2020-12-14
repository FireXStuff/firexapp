from firexapp.submit.console import setup_console_logging
from firexapp.submit.uid import Uid
from logging import INFO, DEBUG, WARNING
import os
import re
import subprocess
import psutil
from firexapp.broker_manager.broker_factory import BrokerFactory
from socket import gethostname
from firexapp.common import poll_until_file_not_empty, poll_until_dir_empty, find_procs
from firexapp.plugins import PLUGGING_ENV_NAME, cdl2list
from firexapp.fileregistry import FileRegistry

logger = setup_console_logging(__name__)

CELERY_LOGS_REGISTRY_KEY = 'celery_logs'
FileRegistry().register_file(CELERY_LOGS_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'celery'))

CELERY_PIDS_REGISTRY_KEY = 'celery_pids'
FileRegistry().register_file(CELERY_PIDS_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(CELERY_LOGS_REGISTRY_KEY), 'pids'))

MICROSERVICE_LOGS_REGISTRY_KEY = 'microservice_logs'
FileRegistry().register_file(MICROSERVICE_LOGS_REGISTRY_KEY, 'microservice_logs')


class CeleryWorkerStartFailed(Exception):
    pass

class CeleryManager(object):
    celery_bin_name = 'celery'

    def __init__(self, plugins=None, logs_dir=None, worker_log_level='debug', cap_concurrency=None,
                 app='firexapp.engine', celery_bin_dir='', env=None, broker=None):

        if not broker:
            self.broker = BrokerFactory.get_broker_url(assert_if_not_set=True)
        self.hostname = gethostname()
        self.plugins = plugins
        self.logs_dir = logs_dir
        self.worker_log_level = worker_log_level
        self.cap_concurrency = cap_concurrency
        self.app = app
        self.celery_bin_dir = celery_bin_dir

        self.env = os.environ.copy()
        self.update_env(self.get_plugins_env(plugins))
        if env:
            self.update_env(env)

        self.pid_files = dict()
        self._celery_logs_dir = None
        self._celery_pids_dir = None
        self._workers_logs_dir = None

    @property
    def celery_bin(self):
        return os.path.join(self.celery_bin_dir, self.celery_bin_name)

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

    @staticmethod
    def get_plugins_env(plugins):
        plugin_files = cdl2list(plugins)
        return {PLUGGING_ENV_NAME: ",".join(plugin_files)}

    @staticmethod
    def get_celery_logs_dir(logs_dir):
        return FileRegistry().get_file(CELERY_LOGS_REGISTRY_KEY, logs_dir)

    @staticmethod
    def get_celery_pids_dir(logs_dir):
        return FileRegistry().get_file(CELERY_PIDS_REGISTRY_KEY, logs_dir)

    @staticmethod
    def get_worker_logs_dir(logs_dir):
        return FileRegistry().get_file(MICROSERVICE_LOGS_REGISTRY_KEY, logs_dir)

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
            cls.log('No pid file found in %s' % pid_file, level=WARNING)
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

            raise CeleryWorkerStartFailed('The worker %s@%s did not come up after %d seconds.'
                                          '\nPlease look into %s for details.'
                                          '%s' % (workername,
                                                  self.hostname,
                                                  timeout,
                                                  stdout_file,
                                                  extra_err_info))
        pid = self.get_pid_from_file(pid_file)
        self.log('pid %d became active' % pid)

    def start(self, workername, queues=None, wait=True, timeout=15*60, concurrency=None, worker_log_level=None,
              app=None, cap_concurrency=None, cwd=None, soft_time_limit=None):

        # Override defaults if applicable
        worker_log_level = worker_log_level if worker_log_level else self.worker_log_level
        app = app if app else self.app
        cap_concurrency = cap_concurrency if cap_concurrency else self.cap_concurrency

        stdout_file = self._get_stdout_file(workername)
        log_file = self._get_worker_log_file(workername)
        pid_file = self._get_pid_file(workername)
        self.pid_files[workername] = pid_file

        cmd = '%s worker --hostname=%s@%%h --app=%s --loglevel=%s ' \
              '--logfile=%s --pidfile=%s --events --without-gossip --without-heartbeat --without-mingle -Ofair' % \
              (self.celery_bin, workername, app, worker_log_level, log_file, pid_file)
        if queues:
            cmd += ' --queues=%s' % queues
        if concurrency:
            cmd += ' --concurrency=%d' % self.cap_cpu_count(concurrency, cap_concurrency)
        if soft_time_limit:
            cmd += f' --soft-time-limit={soft_time_limit}'

        # piping to ts is helpful for debugging if available
        try:
            subprocess.check_call(["which", "ts"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            pass
        else:
            cmd += " | ts '[%Y-%m-%d %H:%M:%S]'"
        cmd += ' &'

        self.log('Starting %s on %s...' % (workername, self.hostname))
        self.log(cmd)

        if cwd:
            self.log('cwd=%s' % cwd)

        with open(stdout_file, 'ab') as fp:
            subprocess.check_call(cmd, shell=True, stdout=fp, stderr=subprocess.STDOUT, env=self.env,
                                  cwd=cwd)

        if wait:
            self.wait_until_active(pid_file=pid_file, timeout=timeout, stdout_file=stdout_file, workername=workername)

    @staticmethod
    def find_procs(pid_file):
        return find_procs('celery', cmdline_contains='--pidfile=%s' % pid_file)

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
        cls.log('Terminating pid %d' % pid, level=INFO)
        p = psutil.Process(pid)
        p.terminate()
        p.wait(timeout=timeout)

    def shutdown(self, timeout=60):
        if self.pid_files:
            name_to_pid_file = self.pid_files
        else:
            # self.pid_files is only populated when starting celery, so if this manager didn't start the celery
            # instance being operated on, fallback to the pid directory.
            name_to_pid_file = {pf: os.path.join(self.celery_pids_dir, pf) for pf in os.listdir(self.celery_pids_dir)}

        for name, pid_file in name_to_pid_file.items():
            self.log('Attempting shutdown of %s' % name)
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
        return poll_until_dir_empty(self.celery_pids_dir, timeout=timeout)
