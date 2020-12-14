# This module serves as a Singleton that will store
# a registry of the ouptut files needed
import json
from firexapp.submit.uid import Uid

import os


class KeyAlreadyRegistered(Exception):
    pass


class KeyNotRegistered(Exception):
    pass


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class FileRegistry(metaclass=Singleton):
    def __init__(self, from_file=None):
        if from_file:
            self.file_registry = self.read_from_file(from_file)
        else:
            self.file_registry = {}

    @classmethod
    def destroy(cls):
        cls._instances = {}

    def register_file(self, key, relative_path):
        if key in self.file_registry:
            raise KeyAlreadyRegistered('%r already registered; callable=%s' % (key, self.file_registry[key]))
        else:
            self.file_registry[key] = relative_path

    def get_file(self, key, uid_or_logsdir):
        try:
            return self.resolve_path(uid_or_logsdir, self.get_relative_path(key))
        except KeyError:
            raise KeyNotRegistered('%r is not registered' % key)

    def get_relative_path(self, key):
        return self.file_registry[key]

    @staticmethod
    def resolve_path(uid_or_logsdir, relative_path):
        logs_dir = uid_or_logsdir.logs_dir if isinstance(uid_or_logsdir, Uid) else uid_or_logsdir
        return os.path.join(logs_dir, relative_path)

    @staticmethod
    def read_from_file(path):
        with open(path) as fp:
            return json.load(fp)

    def dump_to_file(self, path):
        with open(path, 'w') as fp:
            json.dump(self.file_registry, fp, sort_keys=True, indent=2)
