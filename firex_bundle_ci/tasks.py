import subprocess

from firexapp.engine.celery import app
from firexapp.testing.config_base import discover_tests
from celery.utils.log import get_task_logger
import datetime
import os
from firexapp import firex_subprocess
from firexapp.common import silent_mkdir
import lxml.etree as et
from firexkit.result import get_results
from xunitmerge import merge_trees
from firexkit.task import flame

logger = get_task_logger(__name__)


@app.task(returns='flow_test_run_time')
@flame("flow_tests_configs")
@flame("flow_tests_file", os.path.basename)
def RunIntegrationTests(test_output_dir=None, flow_tests_configs=None, flow_tests_file=None, xunit_file_name=None,
                        uid=None, coverage=True, public_runs=False):
    assert flow_tests_configs or flow_tests_file, 'Must provide at least flow_tests_configs or flow_tests_file'
    if not test_output_dir and uid:
        test_output_dir = os.path.join(uid.logs_dir, 'flow_test_logs')

    # if test_output_dir:
    #    self.send_flame_html(test_logs=get_link(get_firex_viewer_url(test_output_dir), 'Test Logs'))

    cmd = ['flow_tests']
    if test_output_dir:
        silent_mkdir(test_output_dir)
        cmd += ['--logs', test_output_dir]
    if flow_tests_configs:
        cmd += ['--config', flow_tests_configs]
    if flow_tests_file:
        cmd += ['--tests', flow_tests_file]
    if xunit_file_name:
        cmd += ['--xunit_file_name', xunit_file_name]
    if coverage:
        cmd += ['--coverage']
    if public_runs:
        cmd += ['--public_runs']
    start = datetime.datetime.now()
    try:
        completed = firex_subprocess.run(cmd, capture_output=True, timeout=6 * 60, check=True, text=True)
    except (firex_subprocess.CommandFailed, subprocess.TimeoutExpired) as e:
        # TimeoutExpired doesn't respect text=True, so we need to decode the output
        stdout = e.stdout
        stderr = e.stderr
        if stdout:
            if not isinstance(stdout, str):
                stdout = stdout.decode()
            logger.error('Stdout:\n' + stdout)
        if stderr:
            if not isinstance(stderr, str):
                stderr = stderr.decode()
            logger.error('Stderr:\n' + stderr)
        raise
    else:
        done = datetime.datetime.now()
        if completed.stdout:
            logger.info('Stdout:\n' + completed.stdout)
        if completed.stderr:
            logger.info('Stderr:\n' + completed.stderr)

    return (done - start).total_seconds()


@app.task(bind=True)
def RunUnitAndIntegrationTests(self, uid):
    ut_promise = self.enqueue_child(RunUnitTests.s(uid))
    it_promise = self.enqueue_child(RunAllIntegrationTests.s(uid))
    self.wait_for_children()
    unit_tests_xunit, unit_tests_coverage_dat = get_results(ut_promise,
                                                            return_keys=('unit_tests_xunit', 'unit_tests_coverage_dat'))

    integration_tests_xunits, integration_tests_coverage_dats = get_results(it_promise,
                                                                            return_keys=('integration_tests_xunits',
                                                                                         'integration_tests_coverage_dats'))

    xunit_result_files = [unit_tests_xunit] + integration_tests_xunits
    coverage_files = [unit_tests_coverage_dat] + integration_tests_coverage_dats
    self.enqueue_child(AggregateXunit.s(uid=uid, xunit_result_files=xunit_result_files))
    self.enqueue_child(AggregateCoverage.s(uid=uid, coverage_files=coverage_files) |
                       GenerateHtmlCoverage.s(coverage_dat_file='@aggregated_coverage_dat'))
    self.wait_for_children()


@app.task(bind=True, returns=('integration_tests_xunits', 'integration_tests_coverage_dats'))
def RunAllIntegrationTests(self, uid,
                           integration_tests_dir='tests/integration_tests/',
                           integration_tests_logs=None, coverage=True, public_runs=False, max_parallel_tests=15):
    if not integration_tests_logs and uid:
        test_output_dir = os.path.join(uid.logs_dir, 'integration_tests_logs')
    else:
        test_output_dir = integration_tests_logs

    parallel_tasks = []
    integration_tests_xunits = []
    integration_tests_coverage_dats = []

    for config in discover_tests(integration_tests_dir):
        test_config_name = config.name
        test_config_filepath = config.filepath
        test_config_output_dir = os.path.join(test_output_dir, test_config_name)
        xunit_file_name = os.path.join(test_config_output_dir, 'xunit_results.xml')
        integration_tests_xunits.append(xunit_file_name)
        integration_tests_coverage_dats.append(os.path.join(test_config_output_dir, '.coverage'))
        parallel_tasks.append(RunIntegrationTests.s(uid=uid,
                                                    flow_tests_configs=test_config_name,
                                                    flow_tests_file=test_config_filepath,
                                                    test_output_dir=test_config_output_dir,
                                                    xunit_file_name=xunit_file_name,
                                                    public_runs=public_runs,
                                                    coverage=coverage))
    if parallel_tasks:
        promises = self.enqueue_in_parallel(parallel_tasks, max_parallel_chains=int(max_parallel_tests))
        if not all([promise.successful() for promise in promises]):
            raise AssertionError('Some tests failed')
    else:
        raise AssertionError('No Integrations tests to run')

    return integration_tests_xunits, integration_tests_coverage_dats


@app.task(returns='coverage_index')
def GenerateHtmlCoverage(uid, coverage_dat_file):
    assert os.path.exists(coverage_dat_file), f'{coverage_dat_file} does not exist'
    env = os.environ.copy()
    env['COVERAGE_FILE'] = coverage_dat_file
    firex_subprocess.run(['coverage', 'html', '--title', 'Code Coverage'], cwd=uid.logs_dir, env=env, check=True)
    coverge_index = os.path.abspath(os.path.join(uid.logs_dir, 'htmlcov/index.html'))
    if os.path.exists(coverge_index):
        logger.print(f'View Coverage at: {coverge_index}')
        return coverge_index


@app.task(returns='aggregated_coverage_dat')
def AggregateCoverage(uid, coverage_files):
    coverage_files = [f for f in coverage_files if os.path.exists(f)]
    aggregated_coverage_dat = os.path.join(uid.logs_dir, 'aggregated_coverage.dat')
    env = os.environ.copy()
    env['COVERAGE_FILE'] = aggregated_coverage_dat
    firex_subprocess.run(['coverage', 'combine'] + coverage_files, check=True, cwd=uid.logs_dir, env=env)
    if os.path.exists(aggregated_coverage_dat):
        return aggregated_coverage_dat


@app.task(returns='xunit_result_files')
def CollectXunits(uid, integration_test_logs=None):
    if not integration_test_logs:
        integration_test_logs = os.path.join(uid.logs_dir, 'integration_tests_logs')
    xunit_result_files = []
    for d in os.listdir(integration_test_logs):
        xml_file = os.path.join(integration_test_logs, d, 'xunit_results.xml')
        if os.path.exists(xml_file):
            xunit_result_files.append(xml_file)
    return xunit_result_files


# noinspection PyPep8Naming
@app.task(returns='aggregated_xunit_results')
# @flame('xunit_results', lambda location: get_link(get_firex_viewer_url(location), "xunit report"))
def AggregateXunit(uid, xunit_result_files):
    if not len(xunit_result_files):
        raise Exception("No xml results files provided")

    xml_trees = []
    for xunit_file in xunit_result_files:
        # load xml file
        if not os.path.isfile(xunit_file):
            raise FileNotFoundError(xunit_file)
        xml_tree = et.parse(xunit_file)

        # handle cases where the root is testsuite not testsuites
        if xml_tree.getroot().tag == "testsuite":
            new_root = et.Element("testsuites")
            new_root.insert(0, xml_tree.getroot())
            # noinspection PyProtectedMember
            xml_tree._setroot(new_root)

        # strip_system_out(xml_tree)
        xml_tree.getroot().attrib.clear()  # merge_trees can barf of float point 'time'
        xml_trees.append(xml_tree)

    merged = merge_trees(*xml_trees)

    # re-compute tests, failures, errors, and time attributes
    tests = int(merged.getroot().xpath("count(testsuite/testcase)"))
    failures = int(merged.getroot().xpath("count(testsuite/testcase/failure)"))
    errors = int(merged.getroot().xpath("count(testsuite/testcase/error)"))
    merged.getroot().attrib.clear()

    merged.getroot().attrib["tests"] = str(tests)
    merged.getroot().attrib["failures"] = str(failures)
    merged.getroot().attrib["errors"] = str(errors)

    aggregated_xunit_results = os.path.join(uid.logs_dir, 'aggregated_xunit_results.xml')
    merged.write(aggregated_xunit_results, encoding='utf-8', xml_declaration=True)
    if os.path.exists(aggregated_xunit_results):
        return aggregated_xunit_results


@app.task(returns=('unit_tests_xunit', 'unit_tests_coverage_dat'))
def RunUnitTests(uid, unit_tests_dir='tests/unit_tests'):
    assert os.path.exists(unit_tests_dir), f'{unit_tests_dir} does not exist'
    unit_tests_xunit = os.path.join(uid.logs_dir, 'unit_tests_xunit_results.xml')
    unit_tests_coverage_dat = os.path.join(uid.logs_dir, 'unit_tests_coverage.dat')
    env = os.environ.copy()
    env['COVERAGE_FILE'] = unit_tests_coverage_dat
    print('--> Run unit-tests and coverage')
    firex_subprocess.run(['coverage', 'run', '-m', 'xmlrunner', 'discover', '-s', unit_tests_dir, '-p', '*_tests.py',
                          '--output-file', unit_tests_xunit],
                         env=env, check=True)
    return unit_tests_xunit, unit_tests_coverage_dat
