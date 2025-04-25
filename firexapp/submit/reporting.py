import traceback
from abc import ABC, abstractmethod
from functools import wraps

from celery.local import PromiseProxy
from celery.result import AsyncResult
from celery.states import SUCCESS
from celery.utils.log import get_task_logger

from firexkit.result import get_task_name_from_result
from firexkit.task import get_current_reports_uids

logger = get_task_logger(__name__)


REL_COMPLETION_REPORT_PATH = 'completion_email.html'

class ReportGenerator(ABC):
    formatters = tuple()
    loaders = tuple()

    @staticmethod
    def pre_run_report(**kwarg):
        """ This runs in the context of __main__ """
        pass

    @abstractmethod
    def add_entry(self, key_name, value, priority, formatters, **extra):
        pass

    @abstractmethod
    def load_data(self, key_name, value, loaders, **extra):
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

    def filter_loaders(self, all_loaders):
        if not self.loaders:
            return all_loaders
        filtered_loaders = {l: all_loaders[l] for l in self.loaders if l in all_loaders}
        if not filtered_loaders:
            return None
        return filtered_loaders


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
                            formatters = report_entry.get("formatters", [])
                            loaders = report_entry.get("loaders", [])
                            key_name = report_entry["key_name"]
                            logger.debug(f"Processing report entry for task {task_name} with key_name {key_name}")
                            if len(loaders) > 0:
                                logger.debug(f'Loading report data for task {task_name}')
                                filtered_loaders = report_gen.filter_loaders(loaders)

                                if filtered_loaders is None:
                                    continue

                                try:
                                    report_gen.load_data(
                                        key_name=key_name,
                                        value=task_ret[key_name] if key_name else task_ret,
                                        loaders=filtered_loaders,
                                        all_task_returns=task_ret,
                                        task_name=task_name,
                                        task_uuid=task_result.id
                                    )
                                    logger.debug(f'Completed loading report data for task {task_name}')
                                except Exception:
                                    logger.error(f'Error during report data loading for task {task_name}...skipping', exc_info=True)
                                    continue
                            if len(formatters) > 0:
                                logger.debug(f'Adding report entry for task {task_name}')
                                filtered_formatters = report_gen.filter_formatters(formatters)

                                if filtered_formatters is None:
                                    continue

                                try:
                                    report_gen.add_entry(
                                        key_name=key_name,
                                        value=task_ret[key_name] if key_name else task_ret,
                                        priority=report_entry["priority"],
                                        formatters=filtered_formatters,
                                        all_task_returns=task_ret,
                                        task_name=task_name,
                                        task_uuid=task_result.id)
                                    logger.debug(f'Completed adding report entry for task {task_name}')
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

    def decorator(func):

        @wraps(func)
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

        if hasattr(func, '__qualname__'):
            return tag_with_report_meta_data(func)

        logger.debug(f"Skipping applying @report since {func} is not a PromiseProxy type")

        return func

    return decorator


def report_data(key_name=None, **loaders):
    """ Use this decorator to indicate what returns to include in the report and how to load it """

    def decorator(func):

        @wraps(func)
        def tag_with_report_meta_data(cls):

            # guard: prevent bad coding by catching bad return key
            if key_name and key_name not in cls.return_keys:
                raise Exception("Task %s does not specify %s using the @returns decorator. "
                                "It cannot be used in @report" % (cls.name, key_name))

            report_data = {
                "key_name": key_name,
                'loaders': loaders,
            }
            if not cls.has_report_meta():
                cls.report_meta = []
            cls.report_meta.append(report_data)
            return cls

        if hasattr(func, '__qualname__'):
            return tag_with_report_meta_data(func)

        logger.debug(f"Skipping applying @report_data since {func} is not a PromiseProxy type")

        return func

    return decorator