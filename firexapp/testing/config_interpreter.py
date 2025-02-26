import sys
import time
import os
import inspect
import subprocess
from datetime import datetime
from typing import Optional, List
import tempfile

from firexapp.common import wait_until
from firexapp.reporters.json_reporter import load_completion_report
from firexkit.resources import get_cloud_ci_install_config_path
from firexapp.submit.submit import get_firex_id_from_output, get_log_dir_from_output
from firexapp.submit.tracking_service import has_flame
from firexapp.submit.install_configs import load_new_install_configs, INSTALL_CONFIGS_ENV_NAME
from firexapp.testing.config_base import InterceptFlowTestConfiguration, FlowTestConfiguration


class ConfigInterpreter:
    execution_directory = None

    tmp_json_file: Optional[str]

    def __init__(self):
        self.profile = False
        self.coverage = False
        self.is_public = False
        self.tmp_json_file = None

    @staticmethod
    def is_submit_command(test_config: FlowTestConfiguration):
        cmd = test_config.initial_firex_options()
        if not cmd:
            return False
        if cmd[0] in "submit":
            return True
        return cmd[0] not in ["list", "info"]

    @staticmethod
    def is_instance_of_intercept(test_config: FlowTestConfiguration):
        base_classes = [cls.__name__ for cls in inspect.getmro(type(test_config))[1:]]
        return InterceptFlowTestConfiguration.__name__ in base_classes

    @staticmethod
    def create_mock_file(results_folder, results_file, test_name, intercept_microservice):
        mock_file_name = test_name + "_mock.py"
        mock_file = os.path.join(results_folder, mock_file_name)
        intercept_task = """
import os
from celery import current_app as app
from firexkit.task import FireXTask


# noinspection PyPep8Naming
@app.task(base=FireXTask)
def {0}(**kwargs):
    local_stuff = locals()
    local_stuff.update(kwargs)
    import pickle

    def str_default(o):
        return str(o)
    from helper import str2file
    file_path = "{1}"
    dir_path = os.path.dirname(file_path)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    with open(file_path, "wb") as f:
        pickle.dump(local_stuff, f)
    if not os.path.isfile(file_path):
        raise Exception("Could not create result files")
    """
        content = intercept_task.format(intercept_microservice, results_file)
        if not os.path.isdir(results_folder):
            os.mkdir(results_folder)
        with open(mock_file, "w") as f:
            f.write(content)
        return mock_file

    def document_viewer(self, file_path: str)->str:
        return file_path

    @staticmethod
    def get_intercept_results_file(flow_test_config):
        return os.path.join(flow_test_config.results_folder, flow_test_config.name + ".results")

    @staticmethod
    def get_test_name(flow_test_config):
        return flow_test_config.__class__.__name__

    def run_integration_test(self, flow_test_config, results_folder):
        # provide sub-folder for testsuite data
        flow_test_config.results_folder = os.path.join(results_folder, flow_test_config.name)
        os.makedirs(flow_test_config.results_folder)
        flow_test_config.std_out = os.path.join(flow_test_config.results_folder, flow_test_config.name + ".stdout.txt")
        flow_test_config.std_err = os.path.join(flow_test_config.results_folder, flow_test_config.name + ".stderr.txt")

        cmd = self.create_cmd(flow_test_config)
        self.run_executable(cmd, flow_test_config)

    def create_cmd(self, flow_test_config) -> List[str]:
        # assemble options, adding/consolidating --external and --sync
        cmd = self.get_exe(flow_test_config)
        cmd += flow_test_config.initial_firex_options()

        plugins = self.collect_plugins(flow_test_config)
        if plugins:
            cmd += ["--plugins", ",".join(plugins)]

        submit_test = self.is_submit_command(flow_test_config)
        if submit_test:
            flow_test_config.logs_link = os.path.join(flow_test_config.results_folder, flow_test_config.name + ".logs")
            cmd += ["--logs_link", flow_test_config.logs_link]
            if getattr(flow_test_config, "sync", True):
                cmd += ["--sync"]
            if has_flame() and getattr(flow_test_config, "flame_terminate_on_complete", True):
                cmd += ["--flame_terminate_on_complete"]
            if (self.is_public # only use public CI install config for is_public runs
                    and not getattr(flow_test_config, 'no_install_config', False)
                    # Some tests supply install configs (via CLI or ENV) for legitimate testing purposes.
                    and '--install_configs' not in cmd
                    and INSTALL_CONFIGS_ENV_NAME not in os.environ):
                # TODO: should merge test-specific install_configs with ci-viewer configs,
                #  since we usually want the ci URLs, even with a test's install_config specifies other stuff.
                cmd += ['--install_configs', get_cloud_ci_install_config_path()]
            if '--json_file' not in cmd:
                self.tmp_json_file = tempfile.mktemp(prefix='firex_json_file_')
                cmd += ['--json_file', self.tmp_json_file]

        return cmd

    def get_exe(self, flow_test_config) -> List[str]:
        import firexapp
        if self.coverage and not hasattr(flow_test_config, 'no_coverage'):
            return ["coverage", "run", "--branch", "--append", "-m", firexapp.__name__]
        if self.profile:
            base_dir = os.path.dirname(firexapp.__file__)
            exe_file = os.path.join(base_dir, "__main__.py")
            return ["python", "-m", "cProfile", "-s", "cumtime", exe_file]
        return ["python", "-m", firexapp.__name__]

    def collect_plugins(self, flow_test_config)->[]:
        # add test file and dynamically generated files to --plugins
        test_src_file = inspect.getfile(flow_test_config.__class__)
        plugins = [
            os.path.realpath(test_src_file),  # must be first to be imported
        ]

        submit_test = self.is_submit_command(flow_test_config)
        if submit_test:
            if self.coverage and not hasattr(flow_test_config, 'no_coverage'):
                # add the coverage plugin to restart celery in coverage mode
                from firexapp.testing import coverage_plugin
                plugins.append(coverage_plugin.__file__)

            if self.is_instance_of_intercept(flow_test_config):
                # create file containing mock and capture microservices
                intercept = flow_test_config.intercept_service()
                if intercept:
                    intercept_results_file = self.get_intercept_results_file(flow_test_config)
                    mock_file = self.create_mock_file(flow_test_config.results_folder,
                                                      intercept_results_file,
                                                      flow_test_config.name, intercept)
                    plugins.append(mock_file)

        # test and use add_plugins=False to explicitly prevent dynamic plugins being added to the test
        extras = getattr(flow_test_config, "add_plugins", [])
        if extras:
            plugins += extras
        if extras is False:
            plugins = []
        return plugins

    def run_executable(self, cmd, flow_test_config):

        # print useful links
        test_src_file = inspect.getfile(flow_test_config.__class__)
        if hasattr(flow_test_config, 'logs_link'):
            print("\tLogs:", self.document_viewer(flow_test_config.logs_link))
        print("\tTest source:", self.document_viewer(test_src_file))
        print("\tStdout:", self.document_viewer(flow_test_config.std_out))
        print("\tStderr:", self.document_viewer(flow_test_config.std_err))

        elapsed_time = None
        verification_time = None

        # run firex
        try:
            with open(flow_test_config.std_out, 'w') as std_out_f, open(flow_test_config.std_err, 'w') as std_err_f:
                start_time = time.monotonic()
                process = subprocess.Popen(cmd, stdout=std_out_f, stderr=std_err_f,
                                           universal_newlines=True, shell=False, cwd=self.execution_directory,
                                           env=os.environ | flow_test_config.get_extra_run_env())
                _, _ = process.communicate(timeout=getattr(flow_test_config, "timeout", None))
                elapsed_time = time.monotonic() - start_time

            verification_start_time = time.monotonic()
            # check for expected return code
            expected_return = flow_test_config.assert_expected_return_code(process.returncode)
            if expected_return is not None:
                raise Exception("assert_expected_return_code should not return. It should assert if needed")

            if self.tmp_json_file and os.path.isfile(self.tmp_json_file):
                flow_test_config.run_data = load_completion_report(self.tmp_json_file)

            if self.is_submit_command(flow_test_config) and process.returncode == 0 and \
                    self.is_instance_of_intercept(flow_test_config) and \
                    flow_test_config.intercept_service():
                # retrieve captured kwargs and validate them
                intercept_results_file = self.get_intercept_results_file(flow_test_config)
                if not os.path.isfile(intercept_results_file):
                    raise FileNotFoundError(intercept_results_file + " was not found. Could not retrieve results.")

                import pickle
                with open(intercept_results_file, 'rb') as results_file_f:
                    captured_options = pickle.load(results_file_f)
                flow_test_config.assert_expected_options(captured_options)

            with open(flow_test_config.std_out, 'r') as std_out_f, open(flow_test_config.std_err, 'r') as std_err_f:
                errors = std_err_f.read().split("\n")
                errors = [line for line in errors if line and not line.startswith("pydev debugger:")]
                flow_test_config.assert_expected_firex_output(std_out_f.read(), "\n".join(errors))
            verification_time = time.monotonic() - verification_start_time
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as e:
            elapsed_time = getattr(e, 'timeout', None)
            print("\t%s! Current wall time: %s" % (type(e).__name__, datetime.now().strftime('%c')), file=sys.stderr)
            verification_start_time = time.monotonic()
            self.cleanup_after_timeout(flow_test_config.std_out, flow_test_config.std_err)
            verification_time = time.monotonic() - verification_start_time
            raise
        except Exception as e:
            print('\tException: {}: {}'.format(type(e).__name__, e), file=sys.stderr)
            raise
        finally:
            self.on_test_exit(flow_test_config.std_out, flow_test_config.std_err)

            try:
                flow_test_config.cleanup()
            except Exception as cleanup_e:
                print(f'Exception during flow test cleanup: {cleanup_e}')

            if self.tmp_json_file and os.path.exists(self.tmp_json_file):
                os.unlink(self.tmp_json_file)

            # report on time
            if elapsed_time is not None:
                msg = "\tTime %.1fs" % elapsed_time
                if verification_time is not None:
                    overhead = " +(%.1fs)" % verification_time
                    if overhead != " +(0.0s)":
                        msg += overhead
                print(msg)

    def cleanup_after_timeout(self, std_out, std_err):
        pass

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def on_test_exit(self, std_out, std_err):
        # Try to print to log directory to help with debugging
        # noinspection PyBroadException
        try:
            with open(std_out, 'r') as std_out_f:
                std_out_content = std_out_f.read()
                firex_id = get_firex_id_from_output(std_out_content)
                if firex_id:
                    print("\tFireX ID: " + firex_id)
                    if self.is_public:
                        install_configs = load_new_install_configs(firex_id,
                                                                   get_log_dir_from_output(std_out_content),
                                                                   get_cloud_ci_install_config_path())
                        print(f'\tLogs URL: {install_configs.get_logs_root_url()}')
                        print(f"\tFlame: {install_configs.run_url}")
        except Exception as e:
            print(e)
            pass
