import contextlib
import datetime
import glob
import io
import os
import tempfile
import unittest

from lxml import etree
from xmlrunner.runner import XMLTestRunner

from firexapp.testing.test_infra import _set_xunit_suite_start

SUITE_START = datetime.datetime(
    2026, 10, 23, 9, 31, tzinfo=datetime.timezone(datetime.timedelta(hours=-7))
)


def _example_flow_test_case() -> type[unittest.TestCase]:
    # Built on demand rather than at module level, so it isn't collected as a test
    # of its own.
    def test_prints_markup(self):
        print("output with <markup> & ]]> in it")

    return type(
        "ExampleFlowTest",
        (unittest.TestCase,),
        {"test_prints_markup": test_prints_markup},
    )


class SetXunitSuiteStartTests(unittest.TestCase):
    def setUp(self):
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        self.tmp_dir = tmp_dir.name

    def _run_xmlrunner(self) -> str:
        runner = XMLTestRunner(
            output=self.tmp_dir, outsuffix="results", stream=io.StringIO()
        )
        # xmlrunner echoes the output it captures to sys.stdout too.
        with contextlib.redirect_stdout(io.StringIO()):
            runner.run(
                unittest.defaultTestLoader.loadTestsFromTestCase(
                    _example_flow_test_case()
                )
            )
        [xunit_path] = glob.glob(os.path.join(self.tmp_dir, "*.xml"))
        return xunit_path

    def test_replaces_xmlrunners_naive_stop_time_with_the_suite_start(self):
        xunit_path = self._run_xmlrunner()
        testsuite = etree.parse(xunit_path).getroot()
        # what's being fixed: the last testcase's stop time, without an offset.
        self.assertIsNone(
            datetime.datetime.fromisoformat(testsuite.get("timestamp")).tzinfo
        )

        _set_xunit_suite_start(xunit_path, SUITE_START)

        rewritten = etree.parse(xunit_path).getroot()
        self.assertEqual("2026-10-23T09:31:00-07:00", rewritten.get("timestamp"))
        self.assertEqual(testsuite.get("time"), rewritten.get("time"))

    def test_preserves_the_rest_of_the_xunit(self):
        xunit_path = self._run_xmlrunner()
        system_out = etree.parse(xunit_path).findtext(".//system-out")
        self.assertIn("<markup> & ]]>", system_out)

        _set_xunit_suite_start(xunit_path, SUITE_START)

        self.assertEqual(system_out, etree.parse(xunit_path).findtext(".//system-out"))
        with open(xunit_path) as f:
            self.assertIn("<![CDATA[", f.read())

    def test_sets_every_testsuite_under_a_testsuites_root(self):
        xunit_path = os.path.join(self.tmp_dir, "xunit.xml")
        with open(xunit_path, "w") as f:
            f.write(
                "<testsuites>"
                '<testsuite name="a" timestamp="2026-10-23T09:41:00"/>'
                '<testsuite name="b" timestamp="2026-10-23T09:42:00"/>'
                "</testsuites>"
            )

        _set_xunit_suite_start(xunit_path, SUITE_START)

        self.assertEqual(
            ["2026-10-23T09:31:00-07:00"] * 2,
            [
                ts.get("timestamp")
                for ts in etree.parse(xunit_path).getroot().iter("testsuite")
            ],
        )
