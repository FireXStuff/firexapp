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
    config_interpreter = ConfigInterpreter()

    @classmethod
    def populate_tests(cls):
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
    parser.add_argument("--logs", '-l', help="The directory to store results and mocks",
                        default=default_results_dir)
    parser.add_argument("--tests", "--test", '-t', dest="tests",
                        help="The directory or python module containing the test configurations. "
                             "Supports comma delimited lists",
                        default=default_test_dir)
    parser.add_argument("--config", "--configs", '-c', dest="config",
                        help="A comma separated list of test configurations to run", default=None)
    parser.add_argument("--xunit_file_name", help="Name of the xunit file", default=None)
    extras = parser.add_mutually_exclusive_group()
    extras.add_argument("--profile", action='store_true', help="Turn on profiling")
    extras.add_argument("--coverage", action='store_true', help="Turn on code coverage. A .coverage file will be "
                                                                "generated in the logs directory")
    parser.add_argument("--no_html", action='store_true', help="Do not generate an html code coverage report. "
                                                               "Used in combination with --coverage")
    parser.add_argument("--public_runs", action='store_true', default=False,
                        help="Should links be generated to point to public flame deployment?")
    args = parser.parse_args()

    if args.coverage:
        # coverage requires eventlet, but firexapp does not
        try:
            import eventlet
        except ModuleNotFoundError:
            print("eventlet is not installed. eventlet is necessary to get code coverage."
                  "Please run again without the --coverage option", file=sys.stderr)
            exit(-1)
    elif args.no_html:
        parser.error("--no_html cannot be used without --coverage")

    # prepare logging directory
    results_directory = os.path.realpath(args.logs)
    if os.path.isdir(results_directory):
        shutil.rmtree(results_directory)
    os.umask(0)
    os.mkdir(results_directory)

    FlowTestInfra.config_interpreter.profile = args.profile
    FlowTestInfra.config_interpreter.coverage = args.coverage
    FlowTestInfra.config_interpreter.is_public = args.public_runs
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

    if args.coverage:
        os.environ["COVERAGE_FILE"] = os.path.join(results_directory, ".coverage")

    import xmlrunner
    success = unittest.main(module=FlowTestInfra.__module__,
                            testRunner=xmlrunner.XMLTestRunner(output=args.logs, outsuffix="results"),
                            argv=sys.argv[:1],
                            exit=False).result.wasSuccessful()

    # let the user decide what to call the output
    if args.xunit_file_name:
        os.rename(orig_output,
                  os.path.join(args.logs, os.path.basename(args.xunit_file_name)))
    if args.coverage:
        print("combining coverage files...", file=sys.stderr)
        coverage_files = [f for f in os.listdir(results_directory) if f.startswith(".coverage")]
        import subprocess
        if coverage_files:
            subprocess.check_output(["coverage", "combine"] + coverage_files,
                                    cwd=results_directory)

            if not args.no_html:
                cov_report = os.path.join(results_directory, "coverage")
                print("Generating Coverage Report...", file=sys.stderr)
                subprocess.check_output(["coverage", "html", "-d", cov_report],
                                        cwd=results_directory)
                print(cov_report, file=sys.stderr)

    sys.exit(not success)


def default_main():
    # determine default location to look for tests
    import firexapp
    module_dir = os.path.dirname(firexapp.__file__)
    root_dir = os.path.dirname(module_dir)
    package_tests_dir = os.path.join(root_dir, "tests", "integration_tests")
    if os.path.isdir(package_tests_dir):
        default_test_location = package_tests_dir
    else:
        default_test_location = "."
    main(default_results_dir=os.path.join(os.getcwd(), "results"),
         default_test_dir=default_test_location)


if __name__ == "__main__":
    default_main()
