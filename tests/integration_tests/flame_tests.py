import abc
import re
import os
import json
import psutil
import signal
import time
import requests
import requests.exceptions
import urllib.parse
import tempfile
from pathlib import Path

from bs4 import BeautifulSoup
import socketio
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.submit.uid import Uid
from firexapp.engine.celery import app
from firexapp.firex_subprocess import check_output
from firexapp.events.model import EXTERNAL_COMMANDS_KEY
from firexkit.chain import returns
from firexkit.task import flame

from firexapp.events.event_aggregator import RunStates
from firex_flame.flame_helper import get_flame_pid, wait_until_pid_not_exist, wait_until, \
    kill_flame, kill_and_wait, json_file_fn, wait_until_path_exist, deep_merge, wait_until_web_request_ok, \
    filter_paths, REVOKE_REASON_KEY
from firex_flame.flame_task_graph import is_task_dict_complete
from firex_flame.model_dumper import get_tasks_slim_file, get_model_full_tasks_by_names, is_dump_complete, \
    get_run_metadata_file, wait_and_get_flame_url, find_flame_model_dir, load_task_representation, load_slim_tasks, \
    get_run_metadata, get_model_slim_tasks_by_names


test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


class FlameFlowTestConfiguration(FlowTestConfiguration):
    __metaclass__ = abc.ABCMeta

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        assert self.run_data
        log_dir = self.run_data.logs_path
        flame_url = wait_and_get_flame_url(log_dir, timeout=10)
        assert flame_url, "Found no Flame URL in logs_dir"
        try:
            self.assert_on_flame_url(log_dir, flame_url)
        finally:
            # do teardown here until there is a real teardown method.
            kill_flame(log_dir)

    @abc.abstractmethod
    def assert_on_flame_url(self, log_dir, flame_url):
        pass

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


def assert_all_match_some_prefix(strs, allowed_prefixes):
    for string in strs:
        if not any(string.startswith(ignore) for ignore in allowed_prefixes):
            raise AssertionError("Found string '%s' matching no expected prefix: %s"
                                 % (string, allowed_prefixes))


def assert_flame_web_ok(flame_url, path):
    alive_url = urllib.parse.urljoin(flame_url, path)
    alive_request = requests.get(alive_url, timeout=10)
    assert alive_request.ok, f'Expected OK response when fetching {alive_url} resource page: {path}'


class FlameLaunchesTest(FlameFlowTestConfiguration):
    """Verifies Flame is launched with nop chain by requesting a couple web endpoints."""

    flame_terminate_on_complete = False

    def __init__(self):
        self.chain = 'nop'
        super().__init__()

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", self.chain]

    def assert_on_flame_url(self, log_dir, flame_url):
        assert get_flame_pid(log_dir) > 0, "Found no pid: %s" % get_flame_pid(log_dir)
        assert_flame_web_ok(flame_url, '/alive')

        root_request = requests.get(flame_url)
        assert root_request.ok, 'Expected OK response when fetching main page: %s' % root_request.status_code

        # Since there is no central_server supplied, expect all resources to be served relatively.
        main_page_bs = BeautifulSoup(root_request.content, 'html.parser')
        main_page_link_hrefs = [l['href'] for l in main_page_bs.find_all('link')]
        main_page_script_srcs = [s['src'] for s in main_page_bs.find_all('script') if 'src' in s]
        page_resource_urls = main_page_link_hrefs + main_page_script_srcs
        assert_all_match_some_prefix(page_resource_urls, ['/', 'https://fonts.googleapis'])

        tasks_api_request = requests.get(flame_url + '/api/tasks')
        assert tasks_api_request.status_code == 200
        task_names = [t['name'] for t in tasks_api_request.json().values()]
        assert self.chain in task_names, "Task API endpoint didn't include executed chain (%s)." % self.chain


class FlameLaunchWithCentralServerTest(FlameFlowTestConfiguration):
    """Verifies Flame UI correctly includes central server when supplied from firexapp."""

    flame_terminate_on_complete = False

    def __init__(self):
        self.central_server = 'http://some_server.com'
        self.central_server_ui_path = '/some/path'
        self.central_server_ui_url = self.central_server + self.central_server_ui_path
        super().__init__()

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop', '--flame_central_server', self.central_server,
                '--flame_central_server_ui_path', self.central_server_ui_path]

    def assert_on_flame_url(self, log_dir, flame_url):
        flame_url = json_file_fn(get_run_metadata_file(firex_logs_dir=log_dir), lambda d: d['flame_url'])
        root_request = requests.get(flame_url)
        assert root_request.ok, 'Expected OK response when fetching main page: %s' % root_request.status_code

        # Since there is no central_server supplied, expect all resources to be served relatively.
        main_page_bs = BeautifulSoup(root_request.content, 'html.parser')
        main_page_link_hrefs = [l['href'] for l in main_page_bs.find_all('link')]
        main_page_script_srcs = [s['src'] for s in main_page_bs.find_all('script') if 'src' in s]
        page_resource_urls = main_page_link_hrefs + main_page_script_srcs
        assert_all_match_some_prefix(page_resource_urls, [self.central_server_ui_url, 'https://fonts.googleapis'])


class FlameTimeoutShutdownTest(FlameFlowTestConfiguration):
    """ Flame shuts itself down when a given timeout is exceeded."""
    sync = False
    flame_timeout = 1

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'sleep', '--sleep', str(self.flame_timeout),
                '--flame_timeout', str(self.flame_timeout)]

    def assert_on_flame_url(self, log_dir, flame_url):
        time.sleep(self.flame_timeout + 3)
        flame_pid = get_flame_pid(log_dir)
        assert not psutil.pid_exists(flame_pid), "Timeout should have killed flame pid, but it still exists."


class FlameSigtermShutdownTest(FlameFlowTestConfiguration):
    """ Flame shutsdown cleanly when getting a sigterm."""

    flame_terminate_on_complete = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'sleep']

    def assert_on_flame_url(self, log_dir, flame_url):
        flame_pid = get_flame_pid(log_dir)
        assert psutil.pid_exists(flame_pid), "Flame pid should exist before being killed by SIGTERM."
        kill_flame(log_dir, sig=signal.SIGTERM)
        assert not psutil.pid_exists(flame_pid), "SIGTERM should have caused flame to terminate, but pid still exists."


class FlameSigintShutdownTest(FlameFlowTestConfiguration):
    """ Flame shutsdown cleanly when getting a sigint."""

    flame_terminate_on_complete = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop']

    def assert_on_flame_url(self, log_dir, flame_url):
        # Test sigint works after run has completed (i.e. after redis is gone).
        from firexapp.broker_manager.redis_manager import RedisManager
        redis_pid_file = RedisManager.get_pid_file(log_dir)
        redis_pid_gone = wait_until(lambda: not os.path.isfile(redis_pid_file), timeout=15, sleep_for=1)
        assert redis_pid_gone, "Expected redis pid to be gone by now due to run completion."

        flame_pid = get_flame_pid(log_dir)
        assert psutil.pid_exists(flame_pid), "Flame pid should exist before being killed by SIGINT."
        kill_flame(log_dir, sig=signal.SIGINT)
        assert not psutil.pid_exists(flame_pid), "SIGINT should have caused flame to terminate, but pid still exists."
        run_metadata = get_run_metadata(firex_logs_dir=log_dir)
        assert run_metadata['flame_recv_complete'], \
            "Expected Flame's receiving thread to have completed gracefully on SIGINT"


def wait_until_model_task_name_exists(log_dir, task_name, timeout=20, sleep_for=1):
    return wait_until_complete_model_tasks_predicate(
        lambda ts: any(t['name'] == task_name for t in ts.values()),
        log_dir, timeout, sleep_for)


def wait_until_model_task_uuid_complete_runstate(log_dir, timeout=20, sleep_for=1):
    return wait_until_complete_model_tasks_predicate(
        lambda ts: all(
            is_task_dict_complete(t) for t in ts.values()),
        log_dir, timeout, sleep_for)


def read_json(file):
    with open(file) as fp:
        return json.load(fp)


def wait_until_complete_model_tasks_predicate(tasks_pred, log_dir, timeout=20, sleep_for=1):
    def until_pred():
        slim_model_file = get_tasks_slim_file(log_dir)
        if not os.path.isfile(slim_model_file):
            return False
        try:
            tasks_by_uuid = read_json(slim_model_file)
        except json.decoder.JSONDecodeError:
            return False
        else:
            return tasks_pred(tasks_by_uuid)

    return wait_until(until_pred, timeout, sleep_for)


def get_task_by_name(log_dir, name):
    tasks_by_name = get_model_slim_tasks_by_names(
        log_dir,
        [name],
    )
    tasks_with_name = tasks_by_name[name]
    named_task_count = len(tasks_with_name)
    assert named_task_count == 1, f"Expecting single task with name '{name}', but found {named_task_count}"
    return tasks_with_name[0]


class FlameRevokeNonExistantUuidTest(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a uuid that doesn't exist. """

    flame_terminate_on_complete = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'nop']

    def assert_on_flame_url(self, log_dir, flame_url):
        sio_client = socketio.Client()
        resp = {'response': None}
        FAILED_EVENT = 'revoke-failed'

        @sio_client.on('revoke-success')
        def revoke_success():
            resp['response'] = 'revoke-success'

        @sio_client.on(FAILED_EVENT)
        def revoke_failed():
            resp['response'] = FAILED_EVENT

        sio_client.connect(flame_url)
        sio_client.emit('revoke-task', data='This is not a UUID.')
        wait_until(lambda: resp['response'] is not None, timeout=15, sleep_for=1)
        sio_client.disconnect()

        assert resp['response'] == FAILED_EVENT, "Expected response %s but received %s" \
                                                 % (FAILED_EVENT, resp['response'])


class FlameRevokeSuccessTest(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a run. """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call revoke while the run is incomplete.
        return ["submit", "--chain", 'sleep', '--sleep', '90']

    def assert_on_flame_url(self, log_dir, flame_url):
        sleep_exists = wait_until_model_task_name_exists(log_dir, 'sleep')
        assert sleep_exists, "Sleep task doesn't exist in the flame rec file, something is wrong with run."
        sleep_task = get_task_by_name(log_dir, 'sleep')
        assert RunStates.is_incomplete_state(sleep_task['state']), f"Expected incomplete sleep, but found {sleep_task['state']}"

        sio_client = socketio.Client()
        resp = {'response': None}
        SUCCESS_EVENT = 'revoke-success'

        @sio_client.on(SUCCESS_EVENT)
        def revoke_success():
            resp['response'] = SUCCESS_EVENT

        @sio_client.on('revoke-failed')
        def revoke_failed():
            resp['response'] = 'revoke-failed'

        sio_client.connect(flame_url)
        sio_client.emit('revoke-task', data=sleep_task['uuid'])
        wait_until(lambda: resp['response'] is not None, timeout=30, sleep_for=1)
        sio_client.disconnect()

        assert resp['response'] == SUCCESS_EVENT, f"Expected response {SUCCESS_EVENT} but received {resp['response']}"

        # Need to re-parse task data, since now it should be revoked.
        sleep_task_after_revoke = get_task_by_name(log_dir, 'sleep')
        sleep_runstate = sleep_task_after_revoke['state']
        assert RunStates.create(sleep_runstate).is_revoke(), f"Expected sleep runstate to be revoked, was {sleep_runstate}"


def _is_revoked(runstate: str) -> bool:
    try:
        return RunStates.create(runstate).is_revoke()
    except ValueError:
        return False

class FlameRevokeRootRestSuccessTest(FlameFlowTestConfiguration):
    """ Uses Flame's REST API to revoke a run. """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call revoke while the run is incomplete.
        return ["submit", "--chain", 'sleep', '--sleep', '90']

    def assert_on_flame_url(self, log_dir, flame_url):
        root_exists = wait_until_model_task_name_exists(log_dir, 'RootTask')
        assert root_exists, "Root task doesn't exist in the flame rec file, something is wrong with run."

        revoke_request = requests.get(urllib.parse.urljoin(flame_url, '/api/revoke'))
        assert revoke_request.ok, f"Unexpected HTTP response to revoke: {revoke_request}"

        root_revoked = wait_until_complete_model_tasks_predicate(
            lambda ts: any(t['name'] == 'RootTask' and _is_revoked(t.get('state')) for t in ts.values()),
            log_dir)
        if not root_revoked:
            root_task_state = get_task_by_name(log_dir, 'RootTask').get('state')
            raise AssertionError(f"Root task not revoked after revoke request, run state: {root_task_state}")


@app.task(bind=True)
def WaitingParent(self, uid):
    wait_until_path_exist(os.path.join(uid.logs_dir, 'wait_file'), timeout=10, sleep_for=0.5)
    self.enqueue_child(Child.s(), block=True)


TEST_TASK_QUERY = [
    {
        "matchCriteria": {"type": "always-select-fields"},
        "selectPaths": ["uuid", "name", "state"]
    },
    {
        "matchCriteria": {
            "type": "equals",
            "value": {"name": "WaitingParent"}
        },
        "selectDescendants": [
            {"type": "equals", "value": {"name": "GrandChild"}}
        ],
    },
]


def find_tasks_by_name(tasks_by_uuid, task_name):
    return [t for t in tasks_by_uuid.values() if t['name'] == task_name]


def find_task_by_name(tasks_by_uuid, task_name):
    tasks = find_tasks_by_name(tasks_by_uuid, task_name)
    if not tasks:
        return None
    return tasks[0]


def _is_task_complete(tasks_by_uuid, task_name):
    named_task = find_task_by_name(tasks_by_uuid, task_name)
    if named_task:
        return RunStates.is_complete_state(named_task.get('state'))
    return False

class FlameSocketIoTaskQueryTest(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a run. """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call revoke while the run is incomplete.
        return ["submit", "--chain", 'WaitingParent']

    def assert_on_flame_url(self, log_dir, flame_url):
        wait_until_web_request_ok(urllib.parse.urljoin(flame_url, '/alive'), timeout=10, sleep_for=1)

        with tempfile.NamedTemporaryFile(mode='w') as temp:
            rep_file_data = {'model_file_name': 'fake-filename.json', 'task_queries': TEST_TASK_QUERY}
            temp.write(json.dumps(rep_file_data, sort_keys=True, indent=2))
            temp.flush()

            def does_waiting_parent_exist():
                return find_task_by_name(load_task_representation(log_dir, temp.name, consider_running=True),
                                         'WaitingParent')

            task = wait_until(does_waiting_parent_exist, 10, 1)
            assert task, "Could not find WaitingParent"
            assert RunStates.is_incomplete_state(task.get('state')), f"Task not in an incomplete stat: {task.get('state')}"

        sio_client = socketio.Client()
        client_tasks_by_uuid = {}

        @sio_client.on('tasks-query-update')
        def _(update_by_uuid):
            client_tasks_by_uuid.update(deep_merge(client_tasks_by_uuid, update_by_uuid))

        sio_client.connect(flame_url)
        sio_client.emit('start-listen-task-query', data={'task_queries': TEST_TASK_QUERY})

        Path(log_dir, 'wait_file').touch() # allow WaitingParent to continue
        wait_until(_is_task_complete, 30, 1, client_tasks_by_uuid, 'WaitingParent')
        sio_client.disconnect()

        assert find_task_by_name(client_tasks_by_uuid, 'WaitingParent'), "Could not find WaitingParent"


class FlameRedisKillCleanupTest(FlameFlowTestConfiguration):
    """ Kills redis and confirms flame kludges run states to be terminal """

    # Don't run with --sync, since this test will revoke the incomplete root task.
    sync = False

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can kill redis and verify the data is clean up.
        return ["submit", "--chain", 'sleep', '--sleep', '30', '--broker_max_retry_attempts', '2']

    def assert_on_flame_url(self, log_dir, flame_url):
        sleep_exists = wait_until_model_task_name_exists(log_dir, 'sleep')
        assert sleep_exists, "Sleep task doesn't exist in the flame rec file, something is wrong with run."

        redis_pid = int(Path(log_dir, Uid.debug_dirname, 'redis', 'redis.pid').read_text())
        redis_killed = kill_and_wait(redis_pid, sig=signal.SIGKILL)
        assert redis_killed, "Failed to kill redis with pid %s" % redis_pid

        wait_until_model_task_uuid_complete_runstate(log_dir)

        sleep_state = get_task_by_name(log_dir, 'sleep')['state']
        assert sleep_state == 'task-incomplete', f"Expected sleep to be incomplete, was: {sleep_state}"

        root_state = get_task_by_name(log_dir, 'RootTask')['state']
        assert root_state == 'task-incomplete', f"Expected RootTask to be incomplete, was: {root_state}"

        flame_killed = wait_until_pid_not_exist(get_flame_pid(log_dir), timeout=4)
        assert not flame_killed, 'Flame killed after redis shutdown -- it should survive.'


class FlameTerminateOnCompleteTest(FlameFlowTestConfiguration):
    """ Tests 'terminate_on_complete' argument """

    # No sync since we'll revoke the run & confirm the web server is no longer active.
    sync = False
    no_coverage = True
    flame_terminate_on_complete = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'sleep', '--sleep', '30', '--flame_terminate_on_complete', 'True',
                # Tests timeout is ignored when terminate_on_complete is set.
                '--flame_timeout', '1']

    def assert_on_flame_url(self, log_dir, flame_url):
        root_task_exists = wait_until_model_task_name_exists(log_dir, 'RootTask')
        assert root_task_exists, "Root task doesn't exist in the flame rec file, something is wrong with run."

        # Make sure the web API is stil up after the timeout, since it should be ignored due to terminate_on_complete.
        time.sleep(1)
        assert_flame_web_ok(flame_url, '/alive')

        root_task = get_task_by_name(log_dir, 'RootTask')
        revoke_reason = 'this is the revoke reason'
        query = urllib.parse.urlencode({REVOKE_REASON_KEY: revoke_reason})
        try:
            assert_flame_web_ok(flame_url, f'/api/revoke/{root_task["uuid"]}?{query}')
        except (AssertionError, requests.exceptions.RequestException):
            # FIXME: Note the server can shut down so fast the HTTP response isn't produced.
            pass

        # Somewhat big timeout since we need to wait for redis to shutdown gracefully, which then causes the
        # the broker processor to shutdown flame gracefully.
        flame_killed = wait_until_pid_not_exist(get_flame_pid(log_dir), timeout=30)
        assert flame_killed, 'Flame not terminated after root revoked, even though terminate_on_complete was supplied.'

        actual_revoke_reason = self.completed_run.revoked_details.reason
        assert actual_revoke_reason, f'Expected {revoke_reason}, found {actual_revoke_reason}'


# noinspection PyUnusedLocal
@app.task
@returns('sum')
def add(op1, op2):
    return int(op1) + int(op2)


class DumpDataOnCompleteTest(FlameFlowTestConfiguration):
    """ Tests task data model dumping performed at end of broker receiver. """

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'add', '--op1', '1', '--op2', '2']

    def assert_on_flame_url(self, log_dir, flame_url):
        dump_complete = wait_until(is_dump_complete, 5, 0.1, log_dir)
        assert dump_complete, "Model dump not complete, can't assert on task data."

        tasks_by_name = get_model_full_tasks_by_names(log_dir, ['add'])
        assert len(tasks_by_name) == 1
        assert len(tasks_by_name['add']) == 1

        query = {
            ('state',): 'task-succeeded',
            ('firex_bound_args', 'op1'): '1',
            ('firex_bound_args', 'op2'): '2',
            ('firex_result', 'sum'): 3,
        }

        found_paths = filter_paths(tasks_by_name, query)['add']
        assert len(found_paths) == 1


class DumpSomeDataOnForceKillTest(FlameFlowTestConfiguration):
    """ Tests task data model dumping performed even when Flame is force terminated. """
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'sleep', '--sleep', '10']

    def assert_on_flame_url(self, log_dir, flame_url):
        sleep_exists = wait_until_model_task_name_exists(log_dir, 'sleep')
        assert sleep_exists
        time.sleep(2) # small time for slim file to be created.
        # Expect run still active due to sleep and sync=False
        kill_flame(log_dir)

        slim_tasks = load_slim_tasks(log_dir)
        assert any(t['name'] == 'sleep' for t in slim_tasks.values()), \
            "Expected slim tasks including 'sleep' to have been dumped."

@app.task(bind=True)
def Parent(self):
    self.enqueue_child(Child.s(), block=True)


@app.task(bind=True)
def Child(self):
    self.enqueue_child(GrandChild.s(), block=True)


@app.task
def GrandChild():
    pass


test_query_file = os.path.join(test_data_dir, 'extra-task-query.json')


class DumpExtraRepDataOnCompleteTest(FlameFlowTestConfiguration):
    """ Tests task data model dumping of flame_extra_task_dump_paths is performed. """

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'Parent', "--flame_extra_task_dump_paths", test_query_file]

    def assert_on_flame_url(self, log_dir, flame_url):
        dump_complete = wait_until(is_dump_complete, 5, 0.1, log_dir)
        assert dump_complete, "Model dump not complete, can't assert on task data."

        dumped_file_data = load_task_representation(log_dir, test_query_file)

        assert len(dumped_file_data) == 1
        single_task = list(dumped_file_data.values())[0]
        assert single_task['name'] == 'Parent'
        assert len(single_task['descendants']) == 1
        assert list(single_task['descendants'].values())[0]['name'] == 'GrandChild'


class QuertyLiveExtraRepData(FlameFlowTestConfiguration):
    """ Tests task data model dumping of flame_extra_task_dump_paths is performed. """

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'Parent', "--flame_extra_task_dump_paths", test_query_file]

    def assert_on_flame_url(self, log_dir, flame_url):
        dump_complete = wait_until(is_dump_complete, 5, 0.1, log_dir)
        assert dump_complete, "Model dump not complete, can't assert on task data."

        extra_dump_file_basename = json.loads(Path(test_query_file).read_text())['model_file_name']
        dumped_file = os.path.join(find_flame_model_dir(log_dir), extra_dump_file_basename)
        dumped_file_data = json.loads(Path(dumped_file).read_text())

        assert len(dumped_file_data) == 1
        single_task = list(dumped_file_data.values())[0]
        assert single_task['name'] == 'Parent'
        assert len(single_task['descendants']) == 1
        assert list(single_task['descendants'].values())[0]['name'] == 'GrandChild'


class FlameLiveMonitorLocalFile(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a run. """

    # Don't run with --sync, since this test needs the flame server up.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call live monitor APIs.
        return ["submit", "--chain", 'sleep', '--sleep', '120']

    def assert_on_flame_url(self, log_dir, flame_url):
        check_live_file_monitoring('localhost', log_dir, flame_url)


class FlameLiveMonitorRemoteFile(FlameFlowTestConfiguration):
    """ Uses Flame's SocketIO API to revoke a run. """

    # Don't run with --sync, since this test needs the flame server up.
    sync = False
    no_coverage = True

    def initial_firex_options(self) -> list:
        # Sleep so that assert_on_flame_url can call live monitor APIs.
        return ["submit", "--chain", 'sleep', '--sleep', '120']

    def assert_on_flame_url(self, log_dir, flame_url):
        check_live_file_monitoring('0.0.0.0', log_dir, flame_url)


def check_live_file_monitoring(host, log_dir, flame_url):
    sleep_exists = wait_until_model_task_name_exists(log_dir, 'sleep')
    assert sleep_exists, "Sleep task doesn't exist in the flame rec file, something is wrong with run."

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        # Open file permissions so flame will allow live file viewing.
        from stat import S_IRUSR, S_IRGRP, S_IROTH
        os.chmod(f.name, S_IRUSR | S_IRGRP | S_IROTH)

        initial_content = '1 2\n'
        f.write(initial_content)
        f.flush()

        sio_client = socketio.Client()

        container = {'content': ''}

        @sio_client.on('file-data')
        def get_file_line(chunk):
            container['content'] += chunk

        try:
            sio_client.connect(flame_url)
            sio_client.emit('start-listen-file', data={'host': host, 'filepath': f.name})

            found_initial = wait_until(lambda: initial_content.strip() in container['content'],
                                       timeout=10, sleep_for=1)
            assert found_initial, "Expected initial content '%s' but was not found in: %s." \
                                  % (initial_content, container['content'])

            update_listen_content = 'update content\n'
            f.write(update_listen_content)
            f.flush()

            found_update = wait_until(lambda: update_listen_content.strip() in container['content'],
                                          timeout=10, sleep_for=1)
            assert found_update, "Expected update content '%s' but was not found in: %s." \
                                 % (update_listen_content, container['content'])

            sio_client.emit('stop-listen-file')
            time.sleep(1)
            after_stop_listen_content = 'some more content\n'
            f.write(after_stop_listen_content)
            f.flush()

            time.sleep(3)
            assert after_stop_listen_content not in container['content'], \
                "Should have stopped listening but found content."
        finally:
            sio_client.disconnect()


def _flame_return_result_fn(x):
    return 2*x

ARG1_VALUE = 'arg1-value'
ARG2_VALUE = 'arg2-default'
CUSTOM_VALUE = 'custom-value'
UNREGISTERED_HTML_VALUE = 'some HTML'
RETURN = 1

@app.task(bind=True)
@flame('arg1')
@flame('arg2')
@flame('custom_key')
@flame('flame_data_result', _flame_return_result_fn)
@returns('flame_data_result')
def FlameDataService(self, uid, arg1, arg2='default'):

    self.send_flame(dict(custom_key=CUSTOM_VALUE))
    self.send_firex_html(unregistered=UNREGISTERED_HTML_VALUE)

    check_output(['/bin/echo', 'hello'])

    return RETURN


class FlameDataServiceTest(FlameFlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'FlameDataService', "--arg1", ARG1_VALUE, '--arg2', ARG2_VALUE]

    def assert_on_flame_url(self, log_dir, flame_url):
        dump_complete = wait_until(is_dump_complete, 5, 0.1, log_dir)
        assert dump_complete, "Model dump not complete, can't assert on task data."

        tasks_by_name = get_model_full_tasks_by_names(log_dir, ['FlameDataService'])
        assert len(tasks_by_name) == 1
        assert len(tasks_by_name['FlameDataService']) == 1

        task = tasks_by_name['FlameDataService'][0]
        service_flame_data = task['flame_data']

        assert service_flame_data['arg1']['value'] == ARG1_VALUE
        assert service_flame_data['arg2']['value'] == ARG2_VALUE
        assert service_flame_data['custom_key']['value'] == CUSTOM_VALUE
        assert service_flame_data['unregistered']['value'] == UNREGISTERED_HTML_VALUE
        assert service_flame_data['flame_data_result']['value'] == _flame_return_result_fn(RETURN)
        assert all(v['type'] == 'html' for v in service_flame_data.values())

        external_command = list(task[EXTERNAL_COMMANDS_KEY].values())[0]
        assert external_command['cmd'] == ['/bin/echo', 'hello']
        assert external_command['result']['completed']
        assert external_command['result']['output'].strip() == 'hello'
        assert external_command['result']['returncode'] == 0


@app.task(bind=True)
@flame('*')
@returns('flame_data_result')
def FlameAbogDataService(self, uid, arg1, arg2='default'):
    return RETURN


class FlameAbogDataServiceTest(FlameFlowTestConfiguration):

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", 'FlameAbogDataService', "--arg1", ARG1_VALUE, '--arg2', ARG2_VALUE]

    def assert_on_flame_url(self, log_dir, flame_url):
        dump_complete = wait_until(is_dump_complete, 5, 0.1, log_dir)
        assert dump_complete, "Model dump not complete, can't assert on task data."

        tasks_by_name = get_model_full_tasks_by_names(log_dir, ['FlameAbogDataService'])
        assert len(tasks_by_name) == 1
        assert len(tasks_by_name['FlameAbogDataService']) == 1

        task = tasks_by_name['FlameAbogDataService'][0]
        service_flame_abog_data = task['flame_data']['*']['value']

        assert 'arg1' in service_flame_abog_data
        assert 'arg2' in service_flame_abog_data
        assert 'flame_data_result' in service_flame_abog_data
        assert str(RETURN) in service_flame_abog_data
