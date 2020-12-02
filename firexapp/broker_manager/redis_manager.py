import json
import os
import secrets
import time
import shlex
import subprocess
from functools import partial

from firexapp.fileregistry import FileRegistry
from firexapp.submit.uid import Uid
from socket import gethostname
from urllib.parse import urlsplit
from logging import ERROR, INFO
from psutil import Process
from pathlib import Path

from firexapp.broker_manager import BrokerManager
from firexapp.common import get_available_port, wait_until, silent_mkdir

REDIS_DIR_REGISTRY_KEY = 'REDIS_DIR_REGISTRY_KEY'
FileRegistry().register_file(REDIS_DIR_REGISTRY_KEY, os.path.join(Uid.debug_dirname, 'redis'))

REDIS_LOG_REGISTRY_KEY = 'REDIS_LOG_REGISTRY_KEY'
FileRegistry().register_file(REDIS_LOG_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(REDIS_DIR_REGISTRY_KEY), 'redis.stdout.txt'))

REDIS_PID_REGISTRY_KEY = 'REDIS_PID_REGISTRY_KEY'
FileRegistry().register_file(REDIS_PID_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(REDIS_DIR_REGISTRY_KEY), 'redis.pid'))

REDIS_METADATA_REGISTRY_KEY = 'REDIS_METADATA_REGISTRY_KEY'
FileRegistry().register_file(REDIS_METADATA_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(REDIS_DIR_REGISTRY_KEY),
                                          'run-metadata.json'))

REDIS_CREDS_REGISTRY_KEY = 'REDIS_CREDS_REGISTRY_KEY'
FileRegistry().register_file(REDIS_CREDS_REGISTRY_KEY,
                             os.path.join(FileRegistry().get_relative_path(REDIS_DIR_REGISTRY_KEY),
                                          'run-credentials.json'))


class RedisDidNotBecomeActive(Exception):
    pass


class RedisPortNotAssigned(Exception):
    pass


class RedisPasswordReadError(Exception):
    pass


class RedisManager(BrokerManager):

    _METADATA_BROKER_URL_KEY = 'broker_url'  # For reading old-style configurations, before the usage of passwords
    _METADATA_BROKER_HOST_KEY = 'broker_host'
    _METADATA_BROKER_PORT_KEY = 'broker_port'
    _BROKER_PASSWORD_KEY = 'broker_password'
    _BROKER_FAILED_AUTH_STR = 'NOAUTH Authentication required'  # Actual output from Redis

    def __init__(self, redis_bin_base, hostname=gethostname(), port=None, logs_dir=None, password=None):
        self.redis_bin_base = redis_bin_base
        self.host = hostname
        self.port = port
        self.logs_dir = logs_dir

        self._password = str(password) if password else secrets.token_urlsafe(32).lstrip('-')
        self._log_file = None
        self._pid_file = None
        self._metadata_file = None
        self._password_file = None
        self._redis_server_bin = os.path.join(redis_bin_base, 'redis-server')

        if logs_dir:
            # Run Redis server from logs directory, so that the command line has the logs path
            redis_bin_dir = os.path.join(logs_dir, 'bin')
            redis_server_bin = os.path.join(redis_bin_dir, os.path.basename(self._redis_server_bin))
            try:
                silent_mkdir(redis_bin_dir)
                os.symlink(self._redis_server_bin, redis_server_bin)
            except FileExistsError:
                # Link was created previously
                pass
            self._redis_server_bin = redis_server_bin

    @property
    def redis_cli_cmd(self):
        return self.get_redis_cli_cmd(self.port)

    def get_redis_cli_cmd(self, port, include_host=False):
        cmd = os.path.join(self.redis_bin_base, 'redis-cli') + ' -p %d -a %s' % (port, self._password)
        if include_host or self.host != gethostname():
            cmd += ' -h %s' % self.host
        return cmd

    @property
    def redis_server_cmd(self):
        return self.get_redis_server_cmd(self.port)

    def get_redis_server_cmd(self, port):
        return self._redis_server_bin + ' --port %d --requirepass %s' \
               % (port, self._password)

    @property
    def broker_url(self):
        return self.get_broker_url(self.port, self.host, self._password)

    @property
    def broker_url_safe_print(self):
        return self.get_broker_url(self.port, self.host, '**')

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

    @staticmethod
    def get_metadata_file(logs_dir):
        return FileRegistry().get_file(REDIS_METADATA_REGISTRY_KEY, logs_dir)

    @staticmethod
    def get_password_file(logs_dir):
        return FileRegistry().get_file(REDIS_CREDS_REGISTRY_KEY, logs_dir)

    @classmethod
    def read_metadata(cls, logs_dir):
        with open(cls.get_metadata_file(logs_dir)) as fp:
            metadata = json.load(fp)
        return metadata

    @classmethod
    def read_password_data(cls, logs_dir):
        try:
            with open(cls.get_password_file(logs_dir)) as fp:
                password_data = json.load(fp)
        except (PermissionError, FileNotFoundError) as e:
            raise RedisPasswordReadError('Cannot get broker password') from e

        return password_data

    @classmethod
    def get_hostname_port_from_logs_dir(cls, logs_dir):
        metadata = cls.read_metadata(logs_dir)
        try:
            return metadata[cls._METADATA_BROKER_HOST_KEY], str(metadata[cls._METADATA_BROKER_PORT_KEY])

        except KeyError as e:
            # Possibly old-style metadata, before broker password usage
            cls.log('Could not get hostname and port directly. Trying old method.', exc_info=e)
            return cls.get_hostname_port_from_url(broker_url=metadata[cls._METADATA_BROKER_URL_KEY])

    @classmethod
    def get_password_from_logs_dir(cls, logs_dir):
        password_data = cls.read_password_data(logs_dir)
        return password_data[cls._BROKER_PASSWORD_KEY]

    @classmethod
    def get_broker_failed_auth_str(cls) -> str:
        return cls._BROKER_FAILED_AUTH_STR

    @classmethod
    def get_broker_url_from_logs_dir(cls, logs_dir):
        hostname, port = cls.get_hostname_port_from_logs_dir(logs_dir)
        password = cls.get_password_from_logs_dir(logs_dir)

        return cls.get_broker_url(port, hostname, password)

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

    @property
    def metadata_file(self):
        if not self._metadata_file and self.logs_dir:
            _metadata_file = self.get_metadata_file(self.logs_dir)
            os.makedirs(os.path.dirname(_metadata_file), exist_ok=True)
            self._metadata_file = _metadata_file
        return self._metadata_file

    @property
    def password_file(self):
        if not self._password_file and self.logs_dir:
            _password_file = self.get_password_file(self.logs_dir)
            os.makedirs(os.path.dirname(_password_file), exist_ok=True)
            self._password_file = _password_file
        return self._password_file

    def create_metadata_file(self):
        if self.metadata_file:
            self.log('Creating %s' % self.metadata_file)
            data = {self._METADATA_BROKER_HOST_KEY: self.host, self._METADATA_BROKER_PORT_KEY: self.port}
            with open(self.metadata_file, 'w') as f:
                json.dump(data, f, sort_keys=True, indent=2)

    def create_password_file(self):
        if self.password_file:
            self.log('Creating %s' % self.password_file)
            data = {self._BROKER_PASSWORD_KEY: str(self._password)}
            # noinspection PyTypeChecker
            with open(self.password_file, 'w',  opener=partial(os.open, mode=0o600)) as f:
                json.dump(data, f, sort_keys=True, indent=2)

    def _start(self, timeout=60):
        try:
            port = self.port
        except RedisPortNotAssigned:
            port = get_available_port()
        self.log('Starting new process (port %d)...' % port)
        cmd = self.get_redis_server_cmd(port) + ' ' + '--loglevel debug ' \
                                                      '--protected-mode no ' \
                                                      '--daemonize yes ' \
                                                      '--timeout 0 ' \
                                                      '--client-output-buffer-limit slave 0 0 0 ' \
                                                      '--client-output-buffer-limit pubsub 0 0 0'
        if self.pid_file:
            cmd += ' --pidfile %s' % self.pid_file
        if self.log_file:
            cmd += ' --logfile %s' % self.log_file
        subprocess.check_call(shlex.split(cmd))
        if not wait_until(os.path.exists, timeout, 1, self.pid_file):
            raise RedisDidNotBecomeActive(f'The Redis pid file {self.pid_file} did not exist within {timeout}s')
        self.wait_until_active(port=port, timeout=timeout)
        self.port = port
        self.create_password_file()
        self.create_metadata_file()
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

    def force_kill(self):
        # noinspection PyBroadException
        try:
            RedisManager.log('force killing...')
            Process(int(Path(self.pid_file).read_text())).kill()
        except Exception:
            RedisManager.log('could not force kill.')

    def get_url(self) -> str:
        return self.broker_url

    def is_alive(self, port=None):
        port = self.port if port is None else port
        try:
            # including the host makes sure the current host can connect to itself via its hostname, like celery will.
            output = self.cli('PING', port=port, include_host=True)
        except subprocess.CalledProcessError:
            return False
        else:
            return output == 'PONG' or self.get_broker_failed_auth_str() in output

    def wait_until_active(self, timeout=60, port=None):
        port = self.port if port is None else port
        timeout_time = time.time() + timeout
        while time.time() < timeout_time:
            if self.is_alive(port=port):
                return
            time.sleep(0.1)
        raise RedisDidNotBecomeActive('Redis Server did not respond after %r seconds' % timeout)

    @staticmethod
    def get_broker_url(port=6379, hostname=gethostname(), password=None):
        preamble = f':{password}@' if password else ''
        return 'redis://%s%s:%d/0' % (preamble, hostname, int(port))

    @staticmethod
    def get_hostname_port_from_url(broker_url):
        url = urlsplit(broker_url)
        return url.hostname, str(url.port) if url.port else url.port

    @staticmethod
    def get_password_from_url(broker_url):
        return urlsplit(broker_url).password

    def get(self, key):
        return self.cli('GET %s' % key)

    def set(self, key, value):
        rc = self.cli('SET %s %s' % (key, value))
        assert rc == 'OK', 'The return value was %s' % rc

    def monitor(self, monitor_file):
        cmd = os.path.join(self.redis_bin_base, 'redis-cli') + ' -p %d -a %s MONITOR &' % (self.port, self._password)
        with open(monitor_file, 'w') as out:
            subprocess.check_call(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT)

    def cli(self, cmd, port=None, include_host=False):
        port = self.port if port is None else port
        null = open(os.devnull, 'w')
        cmd = self.get_redis_cli_cmd(port=port, include_host=include_host) + ' %s' % cmd
        return subprocess.check_output(shlex.split(cmd), stderr=null).decode().strip()

    def __repr__(self):
        return self.broker_url

    def __eq__(self, other):
        return str(other) == self.broker_url
