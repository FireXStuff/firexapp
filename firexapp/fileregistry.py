# This module serves as a Singleton that will store
# a registry of the ouptut files needed
from functools import partial

import os

file_registry = {}


class KeyAlreadyRegistered(Exception):
    pass


class KeyNotRegistered(Exception):
    pass


def default_path(dirpath, filename):
    return os.path.join(dirpath, filename)


def register_file(key, obj, *args, **kwargs):
    if key in file_registry:
        raise KeyAlreadyRegistered('%r already registered; callable=%s' % (key, file_registry[key]))
    else:
        f = partial(obj, *args, **kwargs) if callable(obj) else partial(default_path, filename=obj)
        file_registry[key] = f


def get_file(key, *args, **kwargs):
    try:
        return file_registry[key](*args, **kwargs)
    except KeyError:
        raise KeyNotRegistered('%r is not registered' % key)




