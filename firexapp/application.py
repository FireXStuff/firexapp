import os

from argparse import ArgumentParser, Action, RawTextHelpFormatter
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
        self.arg_parser = None

    def run(self):
        if not self.arg_parser:
            self.arg_parser = self.create_arg_parser()
        arguments, others = self.arg_parser.parse_known_args()

        # run default help
        if not hasattr(arguments, "func"):
            self.arg_parser.print_help()
            self.arg_parser.exit()

        arguments.func(arguments)

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

        return main_parser


class CommaDelimitedListAction(Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        self.is_default = True
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(CommaDelimitedListAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        old_value = getattr(namespace, self.dest) if hasattr(namespace, self.dest) and not self.is_default else ""
        self.is_default = False
        if old_value:
            old_value += ","
        new_value = old_value + values
        setattr(namespace, self.dest, new_value)


plugin_support_parser = ArgumentParser(add_help=False)
plugin_support_parser.add_argument("--external", "--plugins", '-external', '-plugins',
                                   help="Comma delimited list of plugins files to load",
                                   default="",
                                   dest='plugins',
                                   action=CommaDelimitedListAction)
