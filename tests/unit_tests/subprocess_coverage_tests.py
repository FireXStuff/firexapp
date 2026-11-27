import os
import socket
import tempfile
import unittest
from pathlib import Path

from coverage import CoverageData

from firexapp.subprocess_coverage import _COVERAGE_DATA_FILE_RE


class CoverageFilenameTests(unittest.TestCase):
    def test_old_and_new_parallel_filenames(self):
        for filename in (
            ".coverage.worker.example.com.123.456789",
            ".coverage.worker.example.com.123.Xabcdefx",
            ".coverage.worker.example.com.pid123.Xabcdefx",
            ".coverage.worker.example.com.pid123.Xabcdefx.H123456h",
        ):
            with self.subTest(filename=filename):
                match = _COVERAGE_DATA_FILE_RE.fullmatch(filename)
                self.assertIsNotNone(match)
                self.assertEqual(match.group("host"), "worker.example.com")
                self.assertEqual(match.group("pid"), "123")

    def test_installed_coverage_parallel_filename(self):
        with tempfile.TemporaryDirectory() as tempdir:
            data = CoverageData(basename=str(Path(tempdir) / ".coverage"), suffix=True)
            data.add_lines({__file__: {1}})
            data.write()
            match = _COVERAGE_DATA_FILE_RE.fullmatch(Path(data.data_filename()).name)
            self.assertIsNotNone(match)
            self.assertIn(
                match.group("host"),
                (socket.gethostname(), socket.gethostname().replace(".", "_")),
            )
            self.assertEqual(match.group("pid"), str(os.getpid()))
