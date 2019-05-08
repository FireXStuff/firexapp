import os
import time
import shlex
import subprocess
from firexapp.fileregistry import FileRegistry
from firexapp.submit.uid import Uid
from socket import gethostname
from urllib.parse import urlsplit
from logging import ERROR, INFO

from firexapp.broker_manager import BrokerManager
from firexapp.common import get_available_port

REDIS_DIR_REGISTRY_KEY = 'REDIS_DIR_REGISTRY_KEY'
FileRegistry().register_file(REDIS_DIR_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'redis'))

REDIS_LOG_REGISTRY_KEY = 'REDIS_LOG_REGISTRY_KEY'
FileRegistry().register_file(REDIS_LOG_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(REDIS_DIR_REGISTRY_KEY), 'redis.stdout'))

REDIS_PID_REGISTRY_KEY = 'REDIS_PID_REGISTRY_KEY'
FileRegistry().register_file(REDIS_PID_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(REDIS_DIR_REGISTRY_KEY), 'redis.pid'))


class RedisDidNotBecomeActive(Exception):
    pass


class RedisPortNotAssigned(Exception):
    pass


class RedisManager(BrokerManager):

    def __init__(self, redis_bin_base, hostname=gethostname(), port=None, logs_dir=None):
        self.redis_bin_base = redis_bin_base
        self.host = hostname
        self.port = port
        self.logs_dir = logs_dir

        self._log_file = None
        self._pid_file = None

    @property
    def redis_cli_cmd(self):
        return self.get_redis_cli_cmd(self.port)

    def get_redis_cli_cmd(self, port):
        cmd = os.path.join(self.redis_bin_base, 'redis-cli') + ' -p %d' % port
        if self.host != gethostname():
            cmd += ' -h %s' % self.host
        return cmd

    @property
    def redis_server_cmd(self):
        return self.get_redis_server_cmd(self.port)

    def get_redis_server_cmd(self, port):
        return os.path.join(self.redis_bin_base, 'redis-server') + ' --port %d' % port

    @property
    def broker_url(self):
        return self.get_broker_url(self.port, self.host)

    @property
    def port(self):
        if not self.__port:
            raise RedisPortNotAssigned()
        return self.__port

    @port.setter
    def port(self, port):
        self.__port = int(port) if port else port

    @staticmethod
    def get_log_file(logs_dir):
        return FileRegistry().get_file(REDIS_LOG_REGISTRY_KEY, logs_dir)

    @staticmethod
    def get_pid_file(logs_dir):
        return FileRegistry().get_file(REDIS_PID_REGISTRY_KEY, logs_dir)

    @property
    def log_file(self):
        if not self._log_file and self.logs_dir:
            _log_file = self.get_log_file(self.logs_dir)
            os.makedirs(os.path.dirname(_log_file), exist_ok=True)
            self._log_file = _log_file
        return self._log_file

    @property
    def pid_file(self):
        if not self._pid_file and self.logs_dir:
            _pid_file = self.get_pid_file(self.logs_dir)
            os.makedirs(os.path.dirname(_pid_file), exist_ok=True)
            self._pid_file = _pid_file
        return self._pid_file

    def _start(self):
        try:
            port = self.port
        except RedisPortNotAssigned:
            port = get_available_port()
        self.log('Starting new process (port %d)...' % port)
        cmd = self.get_redis_server_cmd(port) + ' --loglevel debug --protected-mode no --daemonize yes'
        if self.pid_file:
            cmd += ' --pidfile %s' % self.pid_file
        if self.log_file:
            cmd += ' --logfile %s' % self.log_file
        subprocess.check_call(shlex.split(cmd))
        self.wait_until_active(port=port)
        self.port = port
        self.log('redis started.')

    def start(self, max_retries=3):
        max_trials = max_retries + 1
        trials = 0

        while True:
            trials += 1
            try:
                self._start()
            except (subprocess.CalledProcessError, RedisDidNotBecomeActive):
                if trials >= max_trials:
                    self.log('Redis did not come up after %d trial(s) (max_trials=%d)..Giving up!' %
                             (trials, max_trials), level=ERROR)
                    raise
                self.log('Redis did not come up after %d trial(s) (max_trials=%d)' % (trials, max_trials), level=INFO)
            else:
                break

    def shutdown(self):
        try:
            RedisManager.log('shutting down...')
            self.cli('shutdown')
        except subprocess.CalledProcessError:
            RedisManager.log('could not shutdown.')

    def get_url(self) -> str:
        return self.broker_url

    def is_alive(self, port=None):
        port = self.port if port is None else port
        try:
            output = self.cli('PING', port=port)
        except subprocess.CalledProcessError:
            return False
        else:
            return output == 'PONG'

    def wait_until_active(self, timeout=60, port=None):
        port = self.port if port is None else port
        timeout_time = time.time() + timeout
        while time.time() < timeout_time:
            if self.is_alive(port=port):
                return
            time.sleep(0.1)
        raise RedisDidNotBecomeActive('Redis Server did not respond after %r seconds' % timeout)

    @staticmethod
    def get_broker_url(port=6379, hostname=gethostname()):
        return 'redis://%s:%d/0' % (hostname, int(port))

    @staticmethod
    def get_hostname_port_from_url(broker_url):
        hostname, port = urlsplit(broker_url).netloc.split(':')
        return hostname, port

    def get(self, key):
        return self.cli('GET %s' % key)

    def set(self, key, value):
        rc = self.cli('SET %s %s' % (key, value))
        assert rc == 'OK', 'The return value was %s' % rc

    def monitor(self, monitor_file):
        cmd = os.path.join(self.redis_bin_base, 'redis-cli') + ' -p %d MONITOR &' % self.port
        with open(monitor_file, 'w') as out:
            subprocess.check_call(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT)

    def cli(self, cmd, port=None):
        port = self.port if port is None else port
        null = open(os.devnull, 'w')
        cmd = self.get_redis_cli_cmd(port=port) + ' %s' % cmd
        return subprocess.check_output(shlex.split(cmd), stderr=null).decode().strip()

    def __repr__(self):
        return self.broker_url

    def __eq__(self, other):
        return str(other) == self.broker_url
