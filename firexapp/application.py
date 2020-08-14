import os
import signal
import tempfile
from argparse import ArgumentParser, RawTextHelpFormatter

from firexapp.plugins import load_plugin_modules, cdl2list
from firexapp.submit.console import setup_console_logging

logger = setup_console_logging(__name__)


def main():
    # Need to call setup_console_logging like this as this module is always called from another.
    setup_console_logging("__main__")
    with tempfile.NamedTemporaryFile(delete=True) as submission_tmp_file:
        from firexapp.submit.submit import SubmitBaseApp
        submit_app = SubmitBaseApp(submission_tmp_file=submission_tmp_file.name)
        app = FireXBaseApp(submit_app=submit_app)
        app.run()


def import_microservices(plugins_files=None, imports: tuple = None) -> []:
    for f in cdl2list(plugins_files):
        if not os.path.isfile(f):
            raise FileNotFoundError(f)

    from firexapp.engine.celery import app

    if not imports:
        imports = app.conf.imports

    for module_name in imports:
        __import__(module_name)

    load_plugin_modules(plugins_files)

    return app.tasks


def get_app_task(task_short_name: str, all_tasks=None):
    task_short_name = task_short_name.strip()
    if all_tasks is None:
        from firexapp.engine.celery import app
        all_tasks = app.tasks

    # maybe it isn't a short name, but a long one
    if task_short_name in all_tasks:
        return all_tasks[task_short_name]

    # Search for an exact match first
    for key, value in all_tasks.items():
        if key.split('.')[-1] == task_short_name:
            return value

    # Let's do a case-insensitive search
    task_name_lower = task_short_name.lower()
    for key, value in all_tasks.items():
        if key.split('.')[-1].lower() == task_name_lower:
            return value

    # Can't find a match
    from celery.exceptions import NotRegistered
    raise NotRegistered(task_short_name)


def get_app_tasks(tasks, all_tasks=None):
    if type(tasks) is str:
        tasks = tasks.split(",")
    return [get_app_task(task, all_tasks) for task in tasks]


class FireXBaseApp:
    def __init__(self, submit_app=None, info_app=None):
        if not info_app:
            from firexapp.info import InfoBaseApp
            info_app = InfoBaseApp()
        self.info_app = info_app

        if not submit_app:
            from firexapp.submit.submit import SubmitBaseApp
            submit_app = SubmitBaseApp()
        self.submit_app = submit_app
        self.arg_parser = None
        self.running_app = None
        self._signal_exit_handler = ExitSignalHandler(self)

    def run(self, sys_argv=None):
        if not self.arg_parser:
            self.arg_parser = self.create_arg_parser()

        try:
            if sys_argv is not None:
                "".join(sys_argv).encode('ascii')
        except UnicodeEncodeError as ue:
            self.arg_parser.error(
                'You entered a non-ascii character at the command line.\n' + str(ue))

        arguments, others = self.arg_parser.parse_known_args(sys_argv)

        # run default help
        if not hasattr(arguments, "func"):
            self.arg_parser.print_help()
            self.arg_parser.exit()
        if self.submit_app.run_submit.__name__ not in arguments.func.__name__:
            if len(others):
                # only submit supports 'other' arguments
                msg = 'Unrecognized arguments: %s' % ' '.join(others)
                self.arg_parser.error(message=msg)
            arguments.func(arguments)
        else:
            self.running_app = self.submit_app
            arguments.func(arguments, others)

    def main_error_exit_handler(self, reason=None):
        if self.running_app and hasattr(self.running_app, self.main_error_exit_handler.__name__):
            self.running_app.main_error_exit_handler(reason=reason)
        exit(-1)

    def create_arg_parser(self, description=None)->ArgumentParser:
        if not description:
            description = """
FireX is a workflow automation and execution engine built using a micro-service oriented design and architecture.
FireX provides a framework to facilitate the automation of the various workflows that are part of every development
and testing processes."""
        main_parser = ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
        sub_parser = main_parser.add_subparsers()

        self.info_app.create_list_sub_parser(sub_parser)
        self.info_app.create_info_sub_parser(sub_parser)
        self.submit_app.create_submit_parser(sub_parser)

        self.arg_parser = main_parser
        return main_parser


class ExitSignalHandler:
    first_warning = "\nExiting due to signal %s"
    second_warning = "\nWe know! Have a little patience for crying out loud!"
    last_warning = "\nFINE! We'll stop. But you might have leaked a celery instance or a broker instance."

    @staticmethod
    def _register_signal_handlers(handler):
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGHUP, handler)

    def __init__(self, app):
        def first_exit_handler(signal_num, _):
            def last_exit_handler(_, __):
                logger.error(self.last_warning)
                exit(-1)

            def second_exit_handler(_, __):
                logger.error(self.second_warning)
                self._register_signal_handlers(last_exit_handler)

            self._register_signal_handlers(second_exit_handler)
            signal_name = signal.Signals(signal_num).name
            logger.error(self.first_warning % signal_name)
            app.main_error_exit_handler(reason=f"Received signal {signal_name}.")

        self._register_signal_handlers(first_exit_handler)
