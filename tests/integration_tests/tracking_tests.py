from firexapp.submit.arguments import whitelist_arguments
from firexapp.submit.tracking_service import TrackingService, get_tracking_services
import firexapp.submit.tracking_service
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run


class TestService(TrackingService):
    test_message = "Test service was started"

    def start(self, args)->{}:
        print(self.test_message)


existing_services = get_tracking_services()
firexapp.submit.tracking_service._services = tuple(list(existing_services) + [TestService()])


class TrackingServiceTest(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "nop"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert TestService.test_message in cmd_output
        assert not cmd_err

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
