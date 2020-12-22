import json
import os
from socket import gethostname

from firexapp.application import get_app_tasks
from firexapp.common import silent_mkdir, create_link
from firexapp.submit.reporting import ReportGenerator
from firexkit.result import get_results
from firexkit.task import convert_to_serializable
from celery.utils.log import get_task_logger
from firexapp.engine.celery import app

logger = get_task_logger(__name__)


class FireXJsonReportGenerator(ReportGenerator):
    formatters = ('json',)

    reporter_dirname = 'json_reporter'
    initial_report_filename = 'initial_report.json'
    completion_report_filename = 'completion_report.json'
    report_link_filename = 'run.json'

    @staticmethod
    def get_common_run_data(uid, chain, submission_dir, argv, original_cli):
        data = {'chain': [t.short_name for t in get_app_tasks(chain)],
                'firex_id': str(uid),
                'logs_path': uid.logs_dir,
                'submission_host': app.conf.mc or gethostname(),
                'submission_dir': submission_dir,
                'submission_cmd': original_cli or list(argv),
                }

        viewers = uid.viewers
        if viewers:
            data.update(viewers)

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

    def pre_run_report(self, uid, chain, submission_dir, argv, original_cli=None, json_file=None, **kwargs):
        data = self.get_common_run_data(uid=uid, chain=chain, submission_dir=submission_dir, argv=argv,
                                        original_cli=original_cli)
        data['completed'] = False
        report_file = os.path.join(uid.logs_dir, self.reporter_dirname, self.initial_report_filename)
        self.write_report_file(data, report_file)

        report_link = os.path.join(uid.logs_dir, self.report_link_filename)
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

    def post_run_report(self, uid, chain, root_id, submission_dir, argv, original_cli=None, json_file=None, **kwargs):
        data = self.get_common_run_data(uid=uid, chain=chain, submission_dir=submission_dir, argv=argv,
                                        original_cli=original_cli)
        data['completed'] = True
        data['results'] = get_results(root_id)
        report_file = os.path.join(uid.logs_dir, self.reporter_dirname, self.completion_report_filename)

        self.write_report_file(data, report_file)

        report_link = os.path.join(uid.logs_dir, self.report_link_filename)
        create_link(report_file, report_link, relative=True)

        if json_file:
            try:
                # This is typically not required, unless post_run ran before pre_run
                create_link(report_link, json_file, delete_link=False, relative=True)
            except FileExistsError:
                # This is expected for most cases
                pass

    def add_entry(self, key_name, value, priority, formatters, **extra):
        pass


def get_completion_report_data(logs_dir):
    report_file = os.path.join(logs_dir, FireXJsonReportGenerator.reporter_dirname,
                               FireXJsonReportGenerator.completion_report_filename)
    with open(report_file) as f:
        return json.load(fp=f)
