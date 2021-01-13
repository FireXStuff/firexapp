import os
import subprocess
import unittest

from firexkit import firex_exceptions
import firexapp.firex_subprocess

TEST_TEXT = 'This is a good test'


class SubprocessRunnerTests(unittest.TestCase):
    output_file = os.path.join('runner_output.txt')

    def test_check_output(self):
        runner = firexapp.firex_subprocess.check_output

        with self.subTest():
            return_val = runner(f'/bin/echo {TEST_TEXT}', copy_file_path=self.output_file)
            self.assertEqual(TEST_TEXT, return_val.strip())
            self.assertTrue(os.path.isfile(self.output_file))
            with open(self.output_file, 'r') as f:
                self.assertEqual(TEST_TEXT, f.readline().strip())
            os.remove(self.output_file)

        with self.subTest():
            with self.assertRaises(firexapp.firex_subprocess.CommandFailed) as ee:
                runner('exit 1', shell=True)
            self.assertIsNotNone(ee.exception.stdout)

        with self.subTest():
            with self.assertRaises(subprocess.TimeoutExpired) as ee:
                runner('cat', timeout=1)
            self.assertIsNotNone(ee.exception.stdout)

        with self.subTest():
            with self.assertRaises(firex_exceptions.FireXInactivityTimeoutExpired) as ee:
                runner('cat', inactivity_timeout=1)
            self.assertIsNotNone(ee.exception.stdout)

        with self.subTest("PYTHONPATH doesn't exist in call's env"):
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True).strip(),
                             '')

        with self.subTest("remove_firex_pythonpath set to False"):
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, remove_firex_pythonpath=False).strip(),
                             os.environ['PYTHONPATH'])

        with self.subTest("user injected some PYTHONPATH"):
            new_path = 'some_path'
            env = os.environ.copy()
            env['PYTHONPATH'] = env['PYTHONPATH'] + ':' + new_path
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, env=env).strip(),
                             new_path)

        with self.subTest("user injected multiple PYTHONPATH"):
            new_path = 'some_path:some_path2'
            env = os.environ.copy()
            env['PYTHONPATH'] = env['PYTHONPATH'] + ':' + new_path
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, env=env).strip(),
                             new_path)

        with self.subTest("user set PYTHONPATH to empty string"):
            env = os.environ.copy()
            del env['PYTHONPATH']
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, env=env).strip(),
                             '')

        with self.subTest("user removed PYTHONPATH"):
            env = os.environ.copy()
            env['PYTHONPATH'] = ''
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, env=env).strip(),
                             '')

    def test_check_call(self):
        runner = firexapp.firex_subprocess.check_call

        with self.subTest():
            return_val = runner(f'/bin/echo {TEST_TEXT}')
            self.assertIsNone(return_val)
            self.assertFalse(os.path.isfile(self.output_file))

        with self.subTest():
            return_val = runner(f'/bin/echo {TEST_TEXT}', file=self.output_file)
            self.assertIsNone(return_val)
            self.assertTrue(os.path.isfile(self.output_file))
            with open(self.output_file, 'r') as f:
                self.assertEqual(TEST_TEXT, f.readline().strip())
            os.remove(self.output_file)

        with self.subTest():
            with self.assertRaises(firexapp.firex_subprocess.CommandFailed) as ee:
                runner('exit 1', shell=True)
            self.assertIsNone(ee.exception.stdout)

        with self.subTest():
            with self.assertRaises(firex_exceptions.FireXInactivityTimeoutExpired) as ee:
                runner('cat', inactivity_timeout=1)
            self.assertIsNone(ee.exception.stdout)

    def test_run(self):
        runner = firexapp.firex_subprocess.run

        with self.subTest():
            return_val = runner(f'/bin/echo {TEST_TEXT}')
            self.assertIsInstance(return_val, subprocess.CompletedProcess)
            self.assertIsNotNone(return_val.stdout)
            self.assertFalse(os.path.isfile(self.output_file))

        with self.subTest():
            return_val = runner(f'/bin/echo {TEST_TEXT}', capture_output=True, copy_file_path=self.output_file)
            self.assertIsInstance(return_val, subprocess.CompletedProcess)
            self.assertEqual(TEST_TEXT, return_val.stdout.strip())
            self.assertTrue(os.path.isfile(self.output_file))
            with open(self.output_file, 'r') as f:
                self.assertEqual(TEST_TEXT, f.readline().strip())
            os.remove(self.output_file)

        with self.subTest():
            return_val = runner('exit 1', shell=True, capture_output=True)
            self.assertIsNotNone(return_val.stderr)

        with self.subTest():
            with self.assertRaises(firexapp.firex_subprocess.CommandFailed) as ee:
                runner('exit 1', shell=True, capture_output=True, check=True)
            self.assertIsNotNone(ee.exception.stdout)

        with self.subTest():
            with self.assertRaises(firex_exceptions.FireXInactivityTimeoutExpired) as ee:
                runner('cat', inactivity_timeout=1, capture_output=False)
            self.assertIsNone(ee.exception.stdout)