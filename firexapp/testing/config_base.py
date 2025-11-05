import os
import abc
import sys
import inspect
from importlib import import_module
from typing import Optional

from firexapp.reporters.json_reporter import FireXRunData


class FlowTestConfiguration(object):
    __metaclass__ = abc.ABCMeta

    run_data: Optional[FireXRunData]

    def __init__(self):
        self.results_folder = ""
        self.run_data = None

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def completed_run(self) -> FireXRunData:
        assert self.run_data
        if self.run_data.completed:
            return self.run_data
        return self.run_data.reload()

    @abc.abstractmethod
    def initial_firex_options(self)->list:
        pass

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert not cmd_err, f'Unexpected stderr: {cmd_err}'

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

    def cleanup(self):
        pass

    @staticmethod
    def get_extra_run_env():
        return {}


def assert_is_bad_run(ret_value):
    assert ret_value != 0, "This test should have a FAILURE return code, but returned 0"


def assert_is_good_run(ret_value):
    assert ret_value == 0, "Test expects a CLEAN run, but returned %s. " \
                           "Check the err output to see what went wrong." % str(ret_value)


def skip_test(cls):
    setattr(cls, "skip_test", True)
    return cls


def discover_tests(tests, config_filter="") -> list:
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

    if not configs:
        raise Exception(f'No test configs found in {tests}')

    [print("Skipping " + config.__class__.__name__, file=sys.stderr) for config in configs if hasattr(config,
                                                                                                      "skip_test")]
    configs = [config for config in configs if not hasattr(config, "skip_test")]

    return configs


def import_test_configs(path) -> list[FlowTestConfiguration]:
    # dynamically load module
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if (
        __file__ in path or
        "pycache" in path or  # We don't need to look at the cache
        os.path.basename(path) == "data" # By convention, a "data" directory will contain artifacts for the tests
    ):
        return []

    config_objects = []
    if (
        path.endswith('.py')
        and os.path.basename(__file__) != os.path.basename(path)
        and os.path.isfile(path)
    ):
        sys.path.append(os.path.dirname(os.path.abspath(path)))
        module = import_module(os.path.splitext(os.path.basename(path))[0])

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if FlowTestConfiguration.__name__ in [cls.__name__ for cls in inspect.getmro(obj)[1:]] and \
                            not inspect.isabstract(obj) and '__metaclass__' not in obj.__dict__ and \
                            obj.__module__ == module.__name__:
                config_object = obj()
                config_object.filepath = path
                config_objects.append(config_object)

    elif os.path.isdir(path):
        results_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
        if os.path.normpath(path) != os.path.normpath(results_folder):
            for sub_path in [os.path.join(path, f) for f in os.listdir(path)]:
                config_objects += import_test_configs(sub_path)

    return config_objects
