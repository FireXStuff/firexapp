import sys
import time
import os
import inspect
import subprocess

from firexapp.testing.config_base import InterceptFlowTestConfiguration, FlowTestConfiguration


class ConfigInterpreter:
    execution_directory = None

    def __init__(self):
        self.profile = False

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
    import json

    def str_default(o):
        return str(o)
    from helper import str2file
    file_path = "{1}"
    dir_path = os.path.dirname(file_path)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    with open(file_path, "w") as f:
        f.write(json.dumps(local_stuff, indent=4, default=str_default))
    if not os.path.isfile(file_path):
        raise Exception("Could not create result files")
    """
        content = intercept_task.format(intercept_microservice,
                                        os.path.join(results_folder, results_file))
        with open(mock_file, "w") as f:
            f.write(content)
        return mock_file

    def document_viewer(self, file_path: str)->str:
        return file_path

    @staticmethod
    def get_test_name(flow_test_config):
        return flow_test_config.__class__.__name__

    def run_executable(self, cmd, flow_test_config, results_folder, intercept_results_file):
        test_name = self.get_test_name(flow_test_config)

        # print useful links
        test_src_file = inspect.getfile(flow_test_config.__class__)
        print("\tTest source:", self.document_viewer(test_src_file), file=sys.stderr)
        std_out = os.path.join(results_folder, test_name + ".stdout")
        std_err = os.path.join(results_folder, test_name + ".stderr")
        print("\tStd out:", self.document_viewer(std_out), file=sys.stderr)
        print("\tStd err:", self.document_viewer(std_err), file=sys.stderr)

        elapsed_time = None
        verification_time = None

        # run firex
        try:
            with open(std_out, 'w') as std_out_f, open(std_err, 'w') as std_err_f:
                start_time = time.monotonic()
                process = subprocess.Popen(cmd, stdout=std_out_f, stderr=std_err_f,
                                           universal_newlines=True, shell=False, cwd=self.execution_directory)
                _, _ = process.communicate(timeout=getattr(flow_test_config, "timeout", None))
                elapsed_time = time.monotonic() - start_time

            start_time = time.monotonic()
            # check for expected return code
            expected_return = flow_test_config.assert_expected_return_code(process.returncode)
            if expected_return is not None:
                raise Exception("assert_expected_return_code should not return. It should assert if needed")

            if self.is_submit_command(flow_test_config) and process.returncode is 0 and \
                    self.is_instance_of_intercept(flow_test_config) and \
                    flow_test_config.intercept_service():
                # retrieve captured kwargs and validate them
                if not os.path.isfile(intercept_results_file):
                    raise FileNotFoundError(intercept_results_file + " was not found. Could not retrieve results.")

                import json
                with open(intercept_results_file) as results_file_f:
                    captured_options = json.loads(results_file_f.read())
                flow_test_config.assert_expected_options(captured_options)

            with open(std_out, 'r') as std_out_f, open(std_err, 'r') as std_err_f:
                errors = std_err_f.read().split("\n")
                errors = [line for line in errors if line and not line.startswith("pydev debugger:")]
                flow_test_config.assert_expected_firex_output(std_out_f.read(), "\n".join(errors))
            verification_time = time.monotonic() - start_time
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as e:
            print("\t%s!" % type(e).__name__, file=sys.stderr)
            self.cleanup_after_timeout(std_out, std_err)
            raise
        finally:
            self.on_test_exit(std_out, std_err)

            # report on time
            if elapsed_time is not None:
                msg = "\tTime %.1fs" % elapsed_time
                if verification_time is not None:
                    overhead = " +(%.1fs)" % verification_time
                    if overhead != " +(0.0s)":
                        msg += overhead
                print(msg, file=sys.stderr)

    def cleanup_after_timeout(self, std_out, std_err):
        pass

    def on_test_exit(self, std_out, std_err):
        pass
