import os

from argparse import ArgumentParser, RawTextHelpFormatter
from firexapp.plugins import load_plugin_modules, cdl2list


def main():
    app = FireXBaseApp()
    app.run()


def import_microservices(plugins_files)->[]:
    for f in cdl2list(plugins_files):
        if not os.path.isfile(f):
            raise FileNotFoundError(f)

    from firexapp.engine.celery import app
    for module_name in app.conf.imports:
        __import__(module_name)

    load_plugin_modules(plugins_files)

    return app.tasks


def get_app_task(task_short_name, all_tasks):

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


class FireXBaseApp:
    def __init__(self):
        from firexapp.info import InfoBaseApp
        self.info_app = InfoBaseApp()
        from firexapp.submit import SubmitBaseApp
        self.submit_app = SubmitBaseApp()
        self.arg_parser = None

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
            arguments.func(arguments, others)

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

        return main_parser
