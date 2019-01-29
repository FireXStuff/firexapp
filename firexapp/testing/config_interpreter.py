import os
import inspect
from firexapp.testing.config_base import InterceptFlowTestConfiguration, FlowTestConfiguration


class ConfigInterpreter:
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
    def is_instance_of_intercept(test_config):
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
