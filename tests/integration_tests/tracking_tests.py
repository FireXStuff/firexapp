import os

from firexapp.engine.celery import app
from firexapp.submit.tracking_service import TrackingService, get_tracking_services
import firexapp.submit.tracking_service
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run, assert_is_bad_run
from firexapp.submit.install_configs import INSTALL_CONFIGS_ENV_NAME

test_data_dir = os.path.join(os.path.dirname(__file__), "data", "tracking_services")
tracking_test_install_config_path = os.path.join(test_data_dir, 'install-configs-test-service.json')


def ready_task_msg(count):
    return "%s ready for tasks check." % count


def ready_console_release_msg(count):
    return "%s ready for console release check." % count


class TestService(TrackingService):
    start_message = "Test service was started"

    def __init__(self):
        self.ready_for_tasks_check_count = 0
        self.ready_for_release_console_check_count = 0

    def extra_cli_arguments(self, arg_parser):
        super(TestService, self).extra_cli_arguments(arg_parser)

    def start(self, args, **kwargs)->{}:
        print(self.start_message)
        super(TestService, self).start(args, **kwargs)
        return {"service_success_value": True}

    def ready_for_tasks(self, **kwargs) -> bool:
        print(ready_task_msg(self.ready_for_tasks_check_count))
        ready = self.ready_for_tasks_check_count != 0
        self.ready_for_tasks_check_count += 1
        return ready

    def ready_release_console(self, **kwargs) -> bool:
        print(ready_console_release_msg(self.ready_for_release_console_check_count))
        ready = self.ready_for_release_console_check_count != 0
        self.ready_for_release_console_check_count += 1
        return ready


existing_services = get_tracking_services()
firexapp.submit.tracking_service._services = tuple(list(existing_services) + [TestService()])


@app.task
def service_success(service_success_value=False):
    assert service_success_value is True


class TrackingServiceTest(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "service_success",
                '--install_configs', tracking_test_install_config_path]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert TestService.start_message in cmd_output

        # Tracking service is written to be not ready initially, then ready on second call.
        # Therefore we expect 1 to be present, but not 2 (firexapp should stop checking after all services are ready).
        assert ready_task_msg(0) in cmd_output
        assert ready_task_msg(1) in cmd_output
        assert ready_task_msg(2) not in cmd_output

        assert ready_console_release_msg(0) in cmd_output
        assert ready_console_release_msg(1) in cmd_output
        assert ready_console_release_msg(2) not in cmd_output

        assert not cmd_err, "Unexpected stderr %s" % cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class NoInstallConfigTrackingServiceTest(FlowTestConfiguration):
    """Test all tracking services (e.g. TestService) installed in the env are started when no
        install_config is defined."""

    no_install_config = True

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "service_success",
                '--service_success_value', 'True']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert TestService.start_message in cmd_output
        assert not cmd_err, "Unexpected stderr %s" % cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class TrackingServiceDisabledTest(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "service_success",
                '--service_success_value', 'True',
                '--disable_tracking_services', 'TestService',
                '--install_configs', tracking_test_install_config_path]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert TestService.start_message not in cmd_output

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class TrackingServiceMissingRequiredTest(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        # Test getting the install config from the ENV instead of from the CLI arg.
        os.environ[INSTALL_CONFIGS_ENV_NAME] = os.path.join(test_data_dir, 'install-configs-missing-launcher.json')
        return ["submit", "--chain", "service_success"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert 'Failed to start tracking service' in cmd_err
        assert 'ThisWillBeMissingLauncher' in cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_bad_run(ret_value)
