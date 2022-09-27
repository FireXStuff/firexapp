import json
import os
import getpass
from socket import gethostname
from dataclasses import dataclass, fields
from typing import List, Dict, Optional

from firexapp.application import get_app_tasks
from firexapp.common import silent_mkdir, create_link
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
    viewers: Dict[str, str]
    results: Optional[Dict] = None
    revoked: bool = False


class FireXJsonReportGenerator:
    formatters = ('json',)

    reporter_dirname = 'json_reporter'
    initial_report_filename = 'initial_report.json'
    completion_report_filename = 'completion_report.json'
    report_link_filename = 'run.json'

    @staticmethod
    def get_common_run_data(uid, chain, submission_dir, argv, original_cli, inputs):
        data = {**uid.run_data,
                'chain': [t.short_name for t in get_app_tasks(chain)],
                'logs_path': uid.logs_dir,
                'submission_host': app.conf.mc or gethostname(),
                'submission_dir': submission_dir,
                'submission_cmd': original_cli or list(argv),
                'submitter': getpass.getuser(),
                'inputs': inputs
                }

        viewers = uid.viewers or {}
        data.update(viewers)
        data['viewers'] = viewers

        return data

    @staticmethod
    def write_report_file(data, report_file):
        # Create the json_reporter dir if it doesn't exist
        silent_mkdir(os.path.dirname(report_file))

        with open(report_file, 'w') as f:
            json.dump(convert_to_serializable(data),
                      fp=f,
                      skipkeys=True,
                      sort_keys=True,
                      indent=4)

    @staticmethod
    def create_initial_run_json(uid, chain, submission_dir, argv, original_cli=None, json_file=None,
                                **inputs):
        data = FireXJsonReportGenerator.get_common_run_data(uid=uid,
                                                            chain=chain,
                                                            submission_dir=submission_dir,
                                                            argv=argv,
                                                            original_cli=original_cli,
                                                            inputs=inputs)
        data['completed'] = False
        data['revoked'] = False
        report_file = os.path.join(uid.logs_dir, FireXJsonReportGenerator.reporter_dirname,
                                   FireXJsonReportGenerator.initial_report_filename)
        FireXJsonReportGenerator.write_report_file(data, report_file)

        report_link = os.path.join(uid.logs_dir, FireXJsonReportGenerator.report_link_filename)
        try:
            create_link(report_file, report_link, delete_link=False)
        except FileExistsError:
            logger.debug(f'f{report_link} link already exist; post_run must have already run. '
                         f'No need to link to f{report_file}')

        if json_file:
            try:
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                logger.debug(f'{json_file} link already exist; '
                             f'post_run must have already created the link to {report_link}')

    @staticmethod
    def create_completed_run_json(uid, chain, root_id, submission_dir, argv, run_revoked, original_cli=None,
                                  json_file=None, **inputs):
        data = FireXJsonReportGenerator.get_common_run_data(uid=uid,
                                                            chain=chain,
                                                            submission_dir=submission_dir,
                                                            argv=argv,
                                                            original_cli=original_cli,
                                                            inputs=inputs)
        data['completed'] = True
        data['results'] = get_results(root_id)
        data['revoked'] = run_revoked
        report_file = os.path.join(uid.logs_dir, FireXJsonReportGenerator.reporter_dirname,
                                   FireXJsonReportGenerator.completion_report_filename)

        FireXJsonReportGenerator.write_report_file(data, report_file)

        report_link = os.path.join(uid.logs_dir, FireXJsonReportGenerator.report_link_filename)
        create_link(report_file, report_link, relative=True)

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


def load_completion_report(json_file: str) -> FireXRunData:
    assert os.path.isfile(json_file), f"File doesn't exist: {json_file}"

    with open(json_file) as f:
        run_dict = json.load(fp=f)

    field_names = {f.name for f in fields(FireXRunData)}
    filtered_run_dict = {k: v for k, v in run_dict.items() if k in field_names}
    # TODO: consider using dacite library instead.
    return FireXRunData(**filtered_run_dict)