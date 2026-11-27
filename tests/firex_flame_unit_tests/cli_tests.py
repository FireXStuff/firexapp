import argparse
import subprocess
import sys
import unittest

from firex_flame.flame_helper import str_to_bool
from firex_flame.launcher import FlameLauncher


class BooleanArgumentTests(unittest.TestCase):
    def test_supported_spellings(self):
        for expected, values in (
            (True, ("y", "yes", "t", "true", "on", "1")),
            (False, ("n", "no", "f", "false", "off", "0")),
        ):
            for value in values:
                for spelling in (value, value.upper(), value.title()):
                    with self.subTest(value=spelling):
                        self.assertIs(str_to_bool(spelling), expected)

    def test_invalid_spellings(self):
        for value in ("", "maybe", "2", " true", "false "):
            with self.subTest(value=value), self.assertRaises(ValueError):
                str_to_bool(value)

    def test_launcher_boolean_argument(self):
        parser = argparse.ArgumentParser()
        FlameLauncher().extra_cli_arguments(parser)
        self.assertIsNone(parser.parse_args([]).flame_serve_logs_dir)
        for value, expected in (("YES", True), ("off", False)):
            with self.subTest(value=value):
                args = parser.parse_args(["--flame_serve_logs_dir", value])
                self.assertIs(args.flame_serve_logs_dir, expected)

    def test_flame_cli_boolean_arguments(self):
        # __main__ monkey-patches gevent and Celery at import time. Keep it in
        # its own interpreter so it cannot affect the rest of the test suite.
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from firex_flame.__main__ import _parse_args; "
                    "args = _parse_args(); "
                    "assert args.serve_logs_dir is False; "
                    "assert args.wait_for_webserver is True"
                ),
                "--logs_dir",
                ".",
                "--serve_logs_dir",
                "off",
                "--wait_for_webserver",
                "YES",
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
