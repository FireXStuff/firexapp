import os

from firexapp.firex_subprocess import check_output
from firexkit.proc_utils import kill_old_procs, find_current_user_recent_procs
from firexapp.testing.config_base import assert_is_bad_run

from firexapp.engine.celery import app
from firexapp.testing.config_base import FlowTestConfiguration


@app.task(soft_time_limit=5)
def say_something_repeatedly(uid, timeout, file, say='something'):
    cmd = f'bash -c "for v in {{1..{timeout}}};do echo {say};sleep 1;done"'
    check_output(cmd, file=os.path.join(uid.logs_dir, file))


def get_partial_copied_tmpfilename(logs_dir, chain_name, filename='tmp'):
    partial_filename = str(os.path.basename(__file__)).split('.')[0] + '.' + chain_name + '_' + filename
    files = os.listdir(logs_dir)
    for file in files:
        if file.startswith(partial_filename):
            return file
    raise FileNotFoundError()


def _assert_file_exists_with_content(dir, file):
    p = os.path.join(dir, file)
    assert os.path.isfile(p), f"Expected file '{p}' to exist."
    assert os.path.getsize(p) > 0, f"Expected non-zero file size for {p}"


class CheckOutputNonTempFileCopiedConfig(FlowTestConfiguration):
    file = 'say_results.txt'
    no_coverage = True

    def initial_firex_options(self) -> list:
        return ['submit',
                '--chain', 'say_something_repeatedly',
                '--timeout', '30', # relying on say_something_repeatedly's soft_time_limit to terminate the task early.
                '--file', self.file]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        _assert_file_exists_with_content(self.run_data.logs_path, self.file)

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)


@app.task(soft_time_limit=5)
def run_this():
    run_this_exe = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "just_hang.py")
    return check_output(run_this_exe)


class CheckOutputKillChildSubprocessConfig(FlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ['submit', '--chain', "run_this"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        processes = find_current_user_recent_procs("python", regexstr="just_hang", max_age=0)
        kill_old_procs("python", regexstr="just_hang", keepalive=0)
        assert not processes, "There is a orphaned child process"

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)
