import json
import pprint
from socket import gethostname
from tempfile import NamedTemporaryFile
from firex_keeper import task_query
from firexapp.application import get_app_tasks
from firexapp.common import poll_until_path_exist
from firexapp.tasks.root_tasks import get_configured_root_task
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.engine.celery import app
from firexapp.testing.config_base import FlowTestConfiguration


@app.task(returns='some_output')
def SomePassThroughService(some_input):
    return some_input


def verify_inputs(expected_inputs, inputs):
    assert all(item in inputs.items() for item in expected_inputs.items())


@app.task(returns='common_json_data')
def VerifyInitialJsonReport(uid, chain, submission_dir, json_file, argv, some_input, original_cli=None):
    poll_until_path_exist(json_file)
    with open(json_file) as f:
        json_content = json.load(f)
    common_json_data = {'chain': [t.short_name for t in get_app_tasks(chain)],
                        'firex_id': str(uid),
                        'logs_path': uid.logs_dir,
                        'submission_host': gethostname(),
                        'submission_dir': submission_dir,
                        'submission_cmd': original_cli or list(argv),
                        }

    if uid.viewers:
        common_json_data.update(uid.viewers)
    expected = {**common_json_data, 'completed': False}
    assert all(item in json_content.items() for item in expected.items()), \
        f'Expected\n {pprint.pformat(expected)}\nto be in:\n{pprint.pformat(json_content)}'

    verify_inputs(expected_inputs={'some_input': some_input},
                  inputs=json_content['inputs'])

    # Will return the common json entries to verify they are the same in the final report
    return common_json_data


class JsonReportsGetGenerated(FlowTestConfiguration):
    json_file = NamedTemporaryFile().name

    some_service_arg = 'some_input'
    some_service_arg_value = 'some_value'
    some_service_arg_dict = {some_service_arg: some_service_arg_value}

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", f"{SomePassThroughService.__name__},{VerifyInitialJsonReport.__name__}",
                "--json_file", self.json_file,
                f"--{self.some_service_arg}", self.some_service_arg_value]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_path = get_log_dir_from_output(cmd_output)

        assert task_query.wait_on_keeper_complete(logs_path), "Keeper database is not complete."

        VerifyInitialJsonReport_task = task_query.single_task_by_name(logs_path, VerifyInitialJsonReport.__name__)
        common_json_data = VerifyInitialJsonReport_task.firex_result['common_json_data']

        root_task = task_query.single_task_by_name(logs_path, get_configured_root_task().__name__)
        results = root_task.firex_result

        expected = {'completed': True,
                    'results': results,
                    **common_json_data
                    }

        with open(self.json_file) as f:
            json_content = json.load(f)

        assert all(item in json_content.items() for item in expected.items()), \
            f'Expected {expected} to be in {json_content}'

        verify_inputs(expected_inputs=self.some_service_arg_dict,
                      inputs=json_content['inputs'])

    def assert_expected_return_code(self, ret_value):
        assert ret_value == 0, "Test expects a CLEAN run, but returned %s. " \
                               "Check the err output to see what went wrong." % str(ret_value)
