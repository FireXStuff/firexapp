from dataclasses import dataclass
import os
import datetime
import pytz
import tempfile
from getpass import getuser
import random
import firexkit
import shutil
import re
from typing import Optional
from collections import namedtuple

from firexapp.submit.arguments import whitelist_arguments
from firexkit.permissions import DEFAULT_CHMOD_MODE

BASE_LOGGING_DIR_ENV_VAR_KEY = 'firex_base_logging_dir'

FIREX_ID_DATE_FMT = "%y%m%d-%H%M%S"
FIREX_ID_REGEX = re.compile(r'^FireX-(?P<user>.*?)-(?P<datetime_str>\d{6}-\d{6})-(?P<random_int>\d+)$')


def firex_id_str(user: str, timestamp: datetime.datetime, random_int: int) -> str:
    return f'FireX-{user}-{timestamp.strftime(FIREX_ID_DATE_FMT)}-{random_int}'


@dataclass(frozen=True)
class FireXIdParts:
    user: str
    timestamp: datetime.datetime
    random_int: int

    def firex_id(self):
        return firex_id_str(self.user, self.timestamp, self.random_int)


def get_firex_id_parts(maybe_firex_id: str) -> Optional[FireXIdParts]:
    m = FIREX_ID_REGEX.match(maybe_firex_id)
    if m:
        parts = m.groupdict()
        try:
             tz_unaware_datetime = datetime.datetime.strptime(
                parts['datetime_str'],
                FIREX_ID_DATE_FMT)
        except ValueError:
            pass # invalidate date format.
        else:
            tz_aware_datetime = pytz.utc.localize(tz_unaware_datetime)
            return FireXIdParts(parts['user'], tz_aware_datetime, int(parts['random_int']))
    return None


def is_firex_id(maybe_firex_id: str) -> bool:
    return bool(get_firex_id_parts(maybe_firex_id))


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
            self.identifier = firex_id_str(self.user, self.timestamp, random.randint(1, 65536))
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
        os.makedirs(path)
        return path

    def create_debug_dir(self):
        path = os.path.join(self.logs_dir, self.debug_dirname)
        try:
            os.makedirs(path)
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
        # pkg_resources.resource_filename('firexkit', 'resources') would have been a cleaner way, but
        # pkg_reources is very slow to load
        pkg_resource_dir = os.path.join(os.path.dirname(firexkit.__file__), 'resources')
        resources_dir = self.resources_dir
        shutil.copytree(pkg_resource_dir, resources_dir)
        # Open permissions
        os.chmod(self.resources_dir, DEFAULT_CHMOD_MODE)
        for file in os.listdir(resources_dir):
            os.chmod(os.path.join(resources_dir, file), DEFAULT_CHMOD_MODE)

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

    @property
    def run_data(self):
        return {'firex_id': self.identifier,
                'logs_path': self.logs_dir}


whitelist_arguments("uid")
