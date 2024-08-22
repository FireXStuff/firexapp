import json
import os
import getpass
from socket import gethostname
from dataclasses import dataclass, fields
from tempfile import NamedTemporaryFile
from typing import List, Optional, Any

from celery import bootsteps
from celery.worker.components import Hub

from firexapp.application import get_app_tasks
from firexapp.common import silent_mkdir, create_link
from firexapp.submit.uid import FIREX_ID_REGEX
from firexkit.result import get_results
from firexkit.task import convert_to_serializable
from celery.utils.log import get_task_logger
from firexapp.engine.celery import app

logger = get_task_logger(__name__)

@dataclass
class FireXRunData:
    completed: bool
    chain: List[str]
    firex_id: str
    logs_path: str
    submission_host: str
    submission_dir: str
    submission_cmd: List[str]
    viewers: dict[str, str]
    inputs: dict[str, Any]
    results: Optional[dict[str, Any]] = None
    revoked: bool = False

    def get_result(self, result_key, default=None):
        return (self.results or {}).get('chain_results', {}).get(result_key, default)


def _get_common_run_data(uid, chain, submission_dir, argv, original_cli, inputs):
    if chain:
        chain = [t.short_name for t in get_app_tasks(chain)]
    if not argv:
        argv = []
    data = {
        **uid.run_data,
        'chain': chain,
        'logs_path': uid.logs_dir,
        'submission_host': app.conf.mc or gethostname(),
        'submission_dir': submission_dir,
        'submission_cmd': original_cli or list(argv),
        'submitter': getpass.getuser(),
        'inputs': inputs,
    }

    viewers = uid.viewers or {}
    data.update(viewers)
    data['viewers'] = viewers

    return data


class FireXJsonReportGenerator:
    formatters = ('json',)

    reporter_dirname = 'json_reporter'
    initial_report_filename = 'initial_report.json'
    completion_report_filename = 'completion_report.json'
    report_link_filename = 'run.json'

    @staticmethod
    def write_report_file(data, report_file):
        # Create the json_reporter dir if it doesn't exist
        silent_mkdir(os.path.dirname(report_file))

        # Atomic write, because the completed_run_json can be written from various places, including
        # celery poolworker which runs FireXRunner, celery mainprocess (as a last-resort backup in a bootstep),
        # and in another process (in the sync case). And although the backup method should kick in only after
        # other methods have failed, it's a theoretical possibility they will run concurrently depending
        # on the order of kill signals, especially in the sync case.
        with NamedTemporaryFile(mode='w', encoding='utf-8', dir=os.path.dirname(report_file), delete=False) as f:
            json.dump(convert_to_serializable(data),
                      fp=f,
                      skipkeys=True,
                      sort_keys=True,
                      indent=4)
            f.flush()
            os.fsync(f.fileno())

        os.chmod(f.name, 0o644)
        os.replace(f.name, report_file)

    @staticmethod
    def create_initial_run_json(uid, chain, submission_dir, argv, original_cli=None, json_file=None,
                                **inputs):
        data = _get_common_run_data(
            uid=uid,
            chain=chain,
            submission_dir=submission_dir,
            argv=argv,
            original_cli=original_cli,
            inputs=inputs) | {
                'completed': False,
                'revoked': False,
            }

        initial_report_file = get_initial_run_json_path(uid.logs_dir)
        FireXJsonReportGenerator.write_report_file(data, initial_report_file)

        report_link = os.path.join(uid.logs_dir, FireXJsonReportGenerator.report_link_filename)
        try:
            create_link(initial_report_file, report_link, delete_link=False)
        except FileExistsError:
            logger.debug(f'f{report_link} link already exist. '
                         f'No need to link to f{initial_report_file}')

        if json_file:
            try:
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                logger.debug(f'{json_file} link already exist; '
                             f'post_run must have already created the link to {report_link}')

    @staticmethod
    def create_completed_run_json(uid=None, run_revoked=True, chain=None, root_id=None, submission_dir=None, argv=None,
                                  original_cli=None, json_file=None, logs_dir=None, **inputs):
        if not logs_dir and uid is None:
            raise ValueError(f'At least one of "logs_dir" or "uid" must be supplied')
        logs_dir = uid.logs_dir if uid is not None else logs_dir

        data = None
        try:
            with open(get_initial_run_json_path(logs_dir), encoding='utf-8') as f:
                data = json.load(fp=f)
        except OSError:
            logger.warning(f"Failed to read initial json for {logs_dir}. Creating a minimal completion report.")

        if not data and uid:
            # best effort -- not all termination contexts have access to all this data :/
            data = _get_common_run_data(
                uid=uid,
                chain=chain,
                submission_dir=submission_dir,
                argv=argv,
                original_cli=original_cli,
                inputs=inputs)

        data['completed'] = True
        data['results'] = get_results(root_id) if root_id else None
        data['revoked'] = run_revoked

        completion_report_file = get_completion_run_json_path(logs_dir)
        FireXJsonReportGenerator.write_report_file(data, completion_report_file)

        report_link = os.path.join(logs_dir, FireXJsonReportGenerator.report_link_filename)
        create_link(completion_report_file, report_link, relative=True)

        if json_file:
            try:
                # This is typically not required, unless post_run ran before pre_run
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                # This is expected for most cases
                pass


def get_completion_report_data(logs_dir):
    report_file = os.path.join(logs_dir, FireXJsonReportGenerator.reporter_dirname,
                               FireXJsonReportGenerator.completion_report_filename)
    with open(report_file) as f:
        return json.load(fp=f)


def is_completed_report(json_file: str) -> bool:
    return os.path.basename(os.path.realpath(json_file)) == FireXJsonReportGenerator.completion_report_filename


class RunJsonFileNotFound(AssertionError):
    pass


def load_completion_report(json_file: str) -> FireXRunData:
    if not os.path.isfile(json_file):
        raise RunJsonFileNotFound(f"File doesn't exist: {json_file}")

    with open(json_file) as f:
        run_dict = json.load(fp=f)

    field_names = {f.name for f in fields(FireXRunData)}
    filtered_run_dict = {k: v for k, v in run_dict.items() if k in field_names}
    # TODO: consider using dacite library instead.
    return FireXRunData(**filtered_run_dict)


def get_run_json_path(logs_dir: str) -> str:
    return os.path.join(
        logs_dir,
        FireXJsonReportGenerator.report_link_filename)


def get_initial_run_json_path(logs_dir):
    return os.path.join(
        logs_dir,
        FireXJsonReportGenerator.reporter_dirname,
        FireXJsonReportGenerator.initial_report_filename)


def get_completion_run_json_path(logs_dir):
    return os.path.join(
        logs_dir,
        FireXJsonReportGenerator.reporter_dirname,
        FireXJsonReportGenerator.completion_report_filename)


class ReporterStep(bootsteps.StartStopStep):

    def include_if(self, parent):
        return parent.hostname.startswith(app.conf.primary_worker_name + '@')

    def __init__(self, parent, **kwargs):
        self._logs_dir = None
        logfile = os.path.normpath(kwargs.get('logfile', '') or '')

        while (sp := os.path.split(logfile))[0] != logfile:
            m = FIREX_ID_REGEX.search(sp[1])
            if m:
                self._logs_dir = logfile
                break
            logfile = sp[0]

        super().__init__(parent, **kwargs)

    def stop(self, parent):
        if self._logs_dir and not os.path.exists(get_completion_run_json_path(logs_dir=self._logs_dir)):
            # By now, the report should have been written! Write a default completion report
            FireXJsonReportGenerator.create_completed_run_json(logs_dir=self._logs_dir)

app.steps['worker'].add(ReporterStep)

# We want this step to finish after Pool at least (because a poolworker writes this file in the async case),
# but might as well finish after Hub too
Hub.requires = Hub.requires + (ReporterStep,)
