import sys

import os
import shutil
import argparse
import unittest
from firexapp.testing.config_base import discover_tests
from firexapp.testing.config_interpreter import ConfigInterpreter

TEST_EXE = os.path.realpath(os.path.abspath(__file__))
if not os.path.isfile(TEST_EXE):
    raise Exception("Some import changed the cwd. Can't locate relative files")
TEST_EXE_DIR = os.path.dirname(TEST_EXE)


class FlowTestInfra(unittest.TestCase):
    test_configs = []
    results_dir = None
    failures = 0
    max_acceptable_failures = None
    config_interpreter = None

    @classmethod
    def populate_tests(cls):
        if not cls.config_interpreter:
            cls.config_interpreter = ConfigInterpreter()
        if not cls.results_dir:
            raise Exception("Results directory not set")
        if not cls.test_configs:
            raise Exception("Could not generate tests. No configurations defined\n"
                            "Configs: %s" % str(cls.test_configs))

        # noinspection PyTypeChecker
        cls.max_acceptable_failures = int((len(cls.test_configs) / 2.0) + 1)
        print("Total tests: " + str(len(cls.test_configs)), file=sys.stderr)
        for test_config in cls.test_configs:
            def sub_test(self, config=test_config):
                    print("Running tests " + config.__class__.__name__, file=sys.stderr)
                    try:
                        cls.config_interpreter.run_integration_test(config, self.results_dir)
                        self.assertTrue(True)
                        print("\tPassed", file=sys.stderr)
                    except Exception as e:
                        print("\tFailed", file=sys.stderr)
                        cls.failures += 1
                        raise e

            setattr(cls, 'test_' + test_config.__class__.__name__, sub_test)

    def setUp(self):
        self._outcome.result.dots = False

    def tearDown(self):
        if self.failures > self.max_acceptable_failures:
            print("-"*70, file=sys.stderr)
            print("Run was terrible. Half have failed so far. Skipping the remaining test", file=sys.stderr)
            print("", file=sys.stderr)
            self._outcome.result.shouldStop = True


def main(default_results_dir, default_test_dir):
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs", help="The directory to store results and mocks",
                        default=default_results_dir)
    parser.add_argument("--tests", "--test", dest="tests",
                        help="The directory or python module containing the test configurations. "
                             "Supports comma delimited lists",
                        default=default_test_dir)
    parser.add_argument("--config", "--configs", dest="config",
                        help="A comma separated list of test configurations to run", default=None)
    parser.add_argument("--xunit_file_name", help="Name of the xunit file", default=None)
    parser.add_argument("--profile", action='store_true', help="Turn on profiling")
    args = parser.parse_args()

    # prepare logging directory
    results_directory = args.logs
    if os.path.isdir(results_directory):
        shutil.rmtree(results_directory)
    os.mkdir(results_directory)

    FlowTestInfra.config_interpreter.profile = args.profile
    FlowTestInfra.results_dir = results_directory
    FlowTestInfra.test_configs = discover_tests(args.tests, args.config)
    FlowTestInfra.populate_tests()

    # if running a single suite, rename the test to reflect the suite
    xunit_file_name = "TEST-%s.%s-results.xml" % (FlowTestInfra.__module__, FlowTestInfra.__name__)
    if os.path.isfile(args.tests):
        FlowTestInfra.__name__ = os.path.splitext(os.path.basename(args.tests))[0]
        orig_output = os.path.join(args.logs, xunit_file_name.replace("FlowTestInfra",
                                                                      FlowTestInfra.__name__))
    else:
        orig_output = os.path.join(args.logs, xunit_file_name)

    import xmlrunner
    success = unittest.main(testRunner=xmlrunner.XMLTestRunner(output=args.logs, outsuffix="results"),
                            argv=sys.argv[:1],
                            exit=False).result.wasSuccessful()

    # let the user decide what to call the output
    if args.xunit_file_name:
        os.rename(orig_output,
                  os.path.join(args.logs, os.path.basename(args.xunit_file_name)))
    sys.exit(not success)


if __name__ == "__main__":
    main(default_results_dir=os.path.join(os.getcwd(), "results"),
         default_test_dir=".")
