import shlex
import urllib
from socket import gethostname
from firexapp.broker_manager import BrokerManager
import subprocess
import os
import logging
import time


class RedisManager(BrokerManager):
    def __init__(self, redis_bin_base, logfile=None, pidfile=None, port=6379,
                 hostname=gethostname(), export=True, connect=True, monitor=False, monitorfile=None):
        self.redis_bin_base = redis_bin_base
        self.host = hostname
        self.port = int(port)
        self.broker_url = self.get_broker_url(self.port, self.host)

        self.redis_server_cmd = os.path.join(redis_bin_base, 'redis-server') + ' --port %d' % self.port

        self.redis_cli_cmd = os.path.join(redis_bin_base, 'redis-cli') + ' -p %d' % self.port
        if hostname != gethostname():
            self.redis_cli_cmd += ' -h %s' % hostname

        if connect:
            self.connect(logfile, pidfile)
            if export:
                os.environ['BROKER'] = self.broker_url
                self.log('export BROKER=%s' % self.broker_url, level=logging.INFO)

        if monitor:
            if not monitorfile:
                monitorfile = logfile+'.monitor'
            self.monitor(monitorfile)

    def connect(self, logfile=None, pidfile=None):
        self.log('Starting new process (port %d)...' % self.port)
        cmd = self.redis_server_cmd + ' --loglevel debug --protected-mode no --daemonize yes'
        if pidfile:
            cmd += ' --pidfile %s' % pidfile
        if logfile:
            cmd += ' --logfile %s' % logfile
        subprocess.check_call(shlex.split(cmd))
        self.wait_until_active()
        self.log('redis started.')

    def shutdown(self):
        try:
            RedisManager.log('shutting down...')
            self.cli('shutdown')
        except subprocess.CalledProcessError:
            RedisManager.log('could not shutdown.')

    def isalive(self):
        try:
            output = self.cli('PING')
        except subprocess.CalledProcessError:
            return False
        else:
            return output == 'PONG'

    def wait_until_active(self, max_trials=60):
        trials = 0
        while trials <= max_trials*10:
            if self.isalive():
                return
            time.sleep(0.1)
            trials += 1
        raise AssertionError('Redis Server did not respond after %d trials' % max_trials)

    @staticmethod
    def get_broker_url(port=6379, hostname=gethostname()):
        return 'redis://%s:%d/0' % (hostname, int(port))

    @staticmethod
    def get_hostname_port_from_url(broker_url):
        hostname, port = urllib.parse.urlsplit(broker_url).netloc.split(':')
        return hostname, port

    def get(self, key):
        return self.cli('GET %s' % key)

    def set(self, key, value):
        rc = self.cli('SET %s %s' % (key, value))
        assert rc == 'OK', 'The return value was %s' % rc

    def monitor(self, monitorfile):
        cmd = os.path.join(self.redis_bin_base, 'redis-cli') + ' -p %d MONITOR &' % self.port
        with open(monitorfile, 'w') as out:
            subprocess.check_call(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT)

    def cli(self, cmd):
        null = open(os.devnull, 'w')
        cmd = self.redis_cli_cmd + ' %s' % cmd
        return subprocess.check_output(shlex.split(cmd), stderr=null).decode().strip()

    def __repr__(self):
        return self.broker_url

    def __eq__(self, other):
        return str(other) == self.broker_url
