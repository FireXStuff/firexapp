import os
import abc
import sys
import shutil
import inspect
from importlib import import_module


class FlowTestConfiguration(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.results_folder = ""

    @abc.abstractmethod
    def initial_firex_options(self)->list:
        pass

    @abc.abstractmethod
    def assert_expected_firex_output(self, cmd_output, cmd_err):
        pass

    @abc.abstractmethod
    def assert_expected_return_code(self, ret_value):
        pass


def skip_test(cls):
    setattr(cls, "skip_test", True)
    return cls


def discover_tests(logs: str, tests, config_filter="") -> (list, str):
    # prepare logging directory
    results_directory = logs
    if os.path.isdir(results_directory):
        shutil.rmtree(results_directory)
    os.mkdir(results_directory)

    configs = []
    for tests_path in tests.split(","):

        if not os.path.exists(tests_path):
            print("Error: --tests must be a directory or a python module containing test configs\n"
                  "%s is not recognized" % tests_path, file=sys.stderr)
            exit(-1)
        configs += import_test_configs(tests_path)
    if config_filter:
        filters = [config_filter.strip() for config_filter in config_filter.split(",")]
        configs = [config for config in configs if config.__class__.__name__ in filters]

    [print("Skipping " + config.__class__.__name__, file=sys.stderr) for config in configs if hasattr(config,
                                                                                                      "skip_test")]
    configs = [config for config in configs if not hasattr(config, "skip_test")]

    return configs, results_directory


def import_test_configs(path) -> []:
    # dynamically load module
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if __file__ in path or "pycache" in path:
        return []

    config_objects = []
    if os.path.isfile(path):
        if os.path.splitext(path)[1] != ".py":
            return []
        if os.path.basename(__file__) == os.path.basename(path):
            return []

        sys.path.append(os.path.dirname(os.path.abspath(path)))
        module = import_module(os.path.splitext(os.path.basename(path))[0])

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if FlowTestConfiguration.__name__ in [cls.__name__ for cls in inspect.getmro(obj)[1:]] and \
                            not inspect.isabstract(obj) and '__metaclass__' not in obj.__dict__ and \
                            obj.__module__ == module.__name__:
                config_objects.append(obj())

    elif os.path.isdir(path):
        results_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        if os.path.normpath(path) == os.path.normpath(results_folder):
            return []
        for sub_path in [os.path.join(path, f) for f in os.listdir(path)]:
            config_objects += import_test_configs(sub_path)
    return config_objects
