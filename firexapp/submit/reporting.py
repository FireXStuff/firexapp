from abc import ABC, abstractmethod
from celery.states import SUCCESS
from celery.utils.log import get_task_logger

from firexkit.result import get_task_name_from_result

logger = get_task_logger(__name__)


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
    def post_run_report(self, **kwargs):
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
    def post_run_report(cls, results):
        if results:
            from celery import current_app
            logger.debug("Processing results data for reports")
            for task_result in recurse_results_tree(results):
                # only report on successful tasks
                if task_result.state != SUCCESS:
                    continue

                task_name = get_task_name_from_result(task_result)
                if task_name not in current_app.tasks:
                    continue

                task = current_app.tasks[task_name]
                report_entries = getattr(task, 'report_meta', None)
                if not report_entries:
                    # this task does not have a report decorator
                    continue

                task_ret = task_result.result
                for report_gen in cls.get_generators():
                    for report_entry in report_entries:
                        filtered_formatters = report_gen.filter_formatters(report_entry["formatters"])
                        if filtered_formatters is None:
                            continue

                        key_name = report_entry["key_name"]
                        report_gen.add_entry(
                            key_name=key_name,
                            value=task_ret[key_name] if key_name else task_ret,
                            priority=report_entry["priority"],
                            formatters=filtered_formatters,
                            all_task_returns=task_ret,
                            task_name=task_name,
                            task_uuid=task_result.id)

        for report_gen in cls.get_generators():
            root_task_results = results.result if results else {}
            report_gen.post_run_report(**root_task_results)


def recurse_results_tree(results):
    yield results
    for child in results.children:
        for child_result in recurse_results_tree(child):
            yield child_result


def report(key_name=None, priority=-1, **formatters):
    """ Use this decorator to indicate what returns to include in the report and how to format it """

    def tag_with_report_meta_data(cls):
        # guard: prevent bad coding by catching bad return key
        if key_name and key_name not in cls.return_keys:
            raise Exception("Task %sdoes not specify %s using the @returns decorator. "
                            "It cannot be used in @report" % (cls.name, key_name))

        report_entry = {
            "key_name": key_name,
            'priority': priority,
            'formatters': formatters,
        }
        if not hasattr(cls, 'report_meta'):
            cls.report_meta = []
        cls.report_meta.append(report_entry)
        return cls
    return tag_with_report_meta_data
