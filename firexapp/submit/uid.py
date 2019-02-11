import os
import datetime
import pytz
import tempfile
from getpass import getuser


class Uid(object):
    def __init__(self, identifier=None):
        self.timestamp = datetime.datetime.now(tz=pytz.utc)
        self.user = getuser()
        if identifier:
            self.identifier = identifier
        else:
            self.identifier = 'FireX-%s-%s-%s' % (self.user, self.timestamp.strftime("%y%m%d-%H%M%S"), os.getpid())
        self._logs_dir = None

    @property
    def base_logging_dir(self):
        return tempfile.gettempdir()

    @property
    def logs_dir(self):
        if not self._logs_dir:
            self._logs_dir = self.create_logs_dir()
        return self._logs_dir

    def create_logs_dir(self):
        path = os.path.join(self.base_logging_dir, self.identifier)
        os.makedirs(path, 0o766)
        return path

    def __str__(self):
        return self.identifier

    def __repr__(self):
        return self.identifier

    def __eq__(self, other):
        return str(other) == self.identifier
