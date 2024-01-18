import traceback
from abc import ABC, abstractmethod

from celery.result import AsyncResult
from celery.states import SUCCESS
from celery.utils.log import get_task_logger

from firexkit.result import get_task_name_from_result
from firexkit.task import get_current_reports_uids

logger = get_task_logger(__name__)


REL_COMPLETION_REPORT_PATH = 'completion_email.html'

class ReportGenerator(ABC):
    formatters = tuple()

    @staticmethod
    def pre_run_report(**kwarg):
        """ This runs in the context of __main__ """
        pass

    @abstractmethod
    def add_entry(self, key_name, value, priority, formatters, **extra):
        pass

    @abstractmethod
    def post_run_report(self, root_id=None, **kwargs):
        """ This could runs in the context of __main__ if --sync, other in the context of celery.
            So the instance cannot be assumed be the same as in pre_run_report() """
        pass

    def filter_formatters(self, all_formatters):
        if not self.formatters:
            return all_formatters
        filtered_formatters = {f: all_formatters[f] for f in self.formatters if f in all_formatters}
        if not filtered_formatters:
            return None
        return filtered_formatters


class ReportersRegistry:
    _generators = None

    @classmethod
    def get_generators(cls):
        if not cls._generators:
            cls._generators = [c() for c in ReportGenerator.__subclasses__()]
        return cls._generators

    @classmethod
    def pre_run_report(cls, kwargs):
        for report_gen in cls.get_generators():
            report_gen.pre_run_report(**kwargs)

    @classmethod
    def post_run_report(cls, results, kwargs):
        if kwargs is None:
            kwargs = {}

        if results:
            from celery import current_app
            report_uids = get_current_reports_uids(current_app.backend)
            report_results = [AsyncResult(r, backend=current_app.backend) for r in report_uids]
            logger.debug(f"Processing reports for {report_uids}")
            for task_result in report_results:
                try:
                    # only report on successful tasks
                    if task_result.state != SUCCESS:
                        continue

                    task_name = get_task_name_from_result(task_result)
                    if task_name not in current_app.tasks:
                        continue

                    task = current_app.tasks[task_name]
                    report_entries = getattr(task, 'report_meta')

                    task_ret = task_result.result
                    for report_gen in cls.get_generators():
                        for report_entry in report_entries:
                            filtered_formatters = report_gen.filter_formatters(report_entry["formatters"])
                            if filtered_formatters is None:
                                continue

                            key_name = report_entry["key_name"]
                            try:
                                report_gen.add_entry(
                                    key_name=key_name,
                                    value=task_ret[key_name] if key_name else task_ret,
                                    priority=report_entry["priority"],
                                    formatters=filtered_formatters,
                                    all_task_returns=task_ret,
                                    task_name=task_name,
                                    task_uuid=task_result.id)
                            except Exception:
                                logger.error(f'Error during report generation for task {task_name}...skipping', exc_info=True)
                                continue
                except Exception:
                    logger.error(f"Failed to add report entry for task result {task_result}", exc_info=True)

            logger.debug("Completed processing results data for reports")

        for report_gen in cls.get_generators():
            try:
                logger.debug(f'Running post_run_report for {report_gen}')
                report_gen.post_run_report(root_id=results, **kwargs)
                logger.debug(f'Completed post_run_report for {report_gen}')
            except Exception:
                # Failure in one report generator should not impact another
                logger.error(f'Error in the post_run_report for {report_gen}', exc_info=True)


def report(key_name=None, priority=-1, **formatters):
    """ Use this decorator to indicate what returns to include in the report and how to format it """

    def tag_with_report_meta_data(cls):
        # guard: prevent bad coding by catching bad return key
        if key_name and key_name not in cls.return_keys:
            raise Exception("Task %s does not specify %s using the @returns decorator. "
                            "It cannot be used in @report" % (cls.name, key_name))

        report_entry = {
            "key_name": key_name,
            'priority': priority,
            'formatters': formatters,
        }
        if not cls.has_report_meta():
            cls.report_meta = []
        cls.report_meta.append(report_entry)
        return cls
    return tag_with_report_meta_data
