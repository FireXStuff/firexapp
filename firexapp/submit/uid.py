import os
import datetime
import pytz
import tempfile
from getpass import getuser
import random
import pkg_resources
import shutil

from firexapp.submit.arguments import whitelist_arguments

BASE_LOGGING_DIR_ENV_VAR_KEY = 'firex_base_logging_dir'


class Uid(object):
    debug_dirname = 'firex_internal'
    _resources_dirname = os.path.join(debug_dirname, 'resources')

    def __init__(self, identifier=None):
        self.timestamp = datetime.datetime.now(tz=pytz.utc)
        self.user = getuser()
        if identifier:
            self.identifier = identifier
        else:
            random.seed()
            self.identifier = f'FireX-{self.user}-{self.timestamp.strftime("%y%m%d-%H%M%S")}-{random.randint(1, 65536)}'
        self._base_logging_dir = None
        self._logs_dir = None
        self._debug_dir = None
        self._viewers = {}

    @property
    def base_logging_dir(self):
        if not self._base_logging_dir:
            self._base_logging_dir = os.environ.get(BASE_LOGGING_DIR_ENV_VAR_KEY, tempfile.gettempdir())
        return self._base_logging_dir

    @property
    def logs_dir(self):
        if not self._logs_dir:
            self._logs_dir = self.create_logs_dir()
            self._debug_dir = self.create_debug_dir()
            self.copy_resources()
        return self._logs_dir

    @property
    def debug_dir(self):
        if not self._debug_dir:
            self._debug_dir = self.create_debug_dir()
        return self._debug_dir

    @classmethod
    def get_resources_path(cls, logs_dir):
        return os.path.join(logs_dir, cls._resources_dirname)

    @property
    def resources_dir(self):
        return self.get_resources_path(self.logs_dir)

    def create_logs_dir(self):
        path = os.path.join(self.base_logging_dir, self.identifier)
        os.makedirs(path, 0o777)
        return path

    def create_debug_dir(self):
        path = os.path.join(self.logs_dir, self.debug_dirname)
        try:
            os.makedirs(path, 0o777)
        except FileExistsError:
            # Could have been created by other dependencies (e.g. redis)
            pass
        return path

    def __str__(self):
        return self.identifier

    def __repr__(self):
        return self.identifier

    def __eq__(self, other):
        return str(other) == self.identifier

    def copy_resources(self):
        pkg_resource_dir = pkg_resources.resource_filename('firexkit', 'resources')
        resources_dir = self.resources_dir
        shutil.copytree(pkg_resource_dir, resources_dir)
        # Open permissions
        os.chmod(self.resources_dir, 0o777)
        for file in os.listdir(resources_dir):
            os.chmod(os.path.join(resources_dir, file), 0o666)

    def add_viewers(self, **attrs):
        self._viewers.update(**attrs)

    @property
    def viewers(self):
        return self._viewers

    @property
    def logs_url(self):
        try:
            return self.viewers['logs_url']
        except KeyError:
            return None


whitelist_arguments("uid")
