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
            python_path = 'some value'
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, remove_firex_pythonpath=False,
                                    env={'PYTHONPATH': python_path}).strip(),
                             python_path)

        with self.subTest("user injected some PYTHONPATH"):
            new_path = 'some_path'
            env = {'PYTHONPATH': 'start_path'}
            os.environ['PYTHONPATH'] = env['PYTHONPATH']
            env['PYTHONPATH'] = env['PYTHONPATH'] + ':' + new_path
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, env=env).strip(),
                             new_path)

        with self.subTest("user injected multiple PYTHONPATH"):
            new_path = 'some_path:some_path2'
            env = {'PYTHONPATH': 'start_path'}
            os.environ['PYTHONPATH'] = env['PYTHONPATH']
            env['PYTHONPATH'] = env['PYTHONPATH'] + ':' + new_path
            self.assertEqual(runner(f'/bin/echo $PYTHONPATH', shell=True, env=env).strip(),
                             new_path)

        with self.subTest("user set PYTHONPATH to empty string"):
            env = os.environ.copy()
            env.pop('PYTHONPATH', None)
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

    # Caution: very long-running test!
    def test_stats(self):
        runner = firexapp.firex_subprocess.run
        stats = firexapp.firex_subprocess.ProcStats()
        num_cpu = os.cpu_count()

        cmd = 'seq {num_procs:d} | xargs -P0 -n1 timeout {timeout} md5sum /dev/zero'

        # Test with 50% of cpu
        if num_cpu != 1:
            num_procs = round (num_cpu /2)
            runner(cmd.format(num_procs=num_procs, timeout=5), shell=True, proc_stats=stats, timeout=10)
            expected = 100 * num_procs / num_cpu
            self.assertTrue(expected - 15 < stats.cpu_percent_used < expected + 15,
                            f'Expected {expected}% CPU but got {stats.cpu_percent_used}')
            self.assertNotEqual(0.0, stats.mem_mb_used)
            self.assertNotEqual(0.0, stats.mem_mb_high_wm)


        # 100% CPU
        num_procs = num_cpu
        runner(cmd.format(num_procs=num_procs, timeout=5), shell=True, proc_stats=stats, timeout=10)
        expected = 100
        self.assertTrue(expected - 25 < stats.cpu_percent_used < expected + 25,
                        f'Expected {expected}% CPU but got {stats.cpu_percent_used}')
        self.assertNotEqual(0, stats.mem_mb_used)
        self.assertNotEqual(0, stats.mem_mb_high_wm)

        # 200% CPU
        mem_expected = stats.mem_mb_used * 2
        mem_hw_expected = stats.mem_mb_high_wm * 2
        num_procs = num_cpu * 2
        # Need double time to let procs start / finnish
        runner(cmd.format(num_procs=num_procs, timeout=10), shell=True, proc_stats=stats, timeout=20)
        expected = 100  # CPU cannot go above 100%
        self.assertTrue(expected - 25 < stats.cpu_percent_used < expected + 25,
                        f'Expected {expected}% CPU but got {stats.cpu_percent_used}')
        self.assertTrue(mem_expected * 0.50 <= stats.mem_mb_used < mem_expected * 1.30,  # Loose check due to experience
                        f'Expected {mem_expected} MB memory but got {stats.mem_mb_used}')
        self.assertTrue(mem_hw_expected * 0.50 <= stats.mem_mb_high_wm < mem_hw_expected * 1.30,
                        f'Expected {mem_hw_expected} MB max memory but got {stats.mem_mb_high_wm}')

        # zero running time
        runner('/bin/echo hello', proc_stats=stats, timeout=10)
        self.assertEqual(0, stats.elapsed_time)
        self.assertEqual(0, stats.cpu_percent_used)
        self.assertEqual(0, stats.mem_mb_used)
        self.assertEqual(0, stats.mem_mb_high_wm)
        self.assertNotEqual(0, stats.num_cpu)
