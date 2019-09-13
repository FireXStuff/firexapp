import os
import datetime
import pytz
import tempfile
from getpass import getuser

from firexapp.submit.arguments import whitelist_arguments


class Uid(object):
    debug_dirname = 'debug'

    def __init__(self, identifier=None):
        self.timestamp = datetime.datetime.now(tz=pytz.utc)
        self.user = getuser()
        if identifier:
            self.identifier = identifier
        else:
            self.identifier = 'FireX-%s-%s-%s' % (self.user, self.timestamp.strftime("%y%m%d-%H%M%S"), os.getpid())
        self.base_logging_dir = tempfile.gettempdir()
        self.logs_dir = self.create_subdir(self.base_logging_dir, self.identifier)
        self.debug_dir = self.create_subdir(self.logs_dir, self.debug_dirname)

    @staticmethod
    def create_subdir(base_dir, subdirname):
        path = os.path.join(base_dir, subdirname)
        os.makedirs(path, 0o777)
        return path

    def __str__(self):
        return self.identifier

    def __repr__(self):
        return self.identifier

    def __eq__(self, other):
        return str(other) == self.identifier


whitelist_arguments("uid")
