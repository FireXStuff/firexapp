import sys
import os

from argparse import ArgumentParser, Action, RawTextHelpFormatter
from firexapp.plugins import load_plugin_modules, cdl2list


def main():
    parser = create_arg_parser()
    arguments, others = parser.parse_known_args()

    # run default help
    if not hasattr(arguments, "func"):
        parser.print_help()
        parser.exit()

    arguments.func(arguments)


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


def create_arg_parser(description=None)->ArgumentParser:
    if not description:
        description = """
FireX is a workflow automation and execution engine built using a micro-service oriented design and architecture.
FireX provides a framework to facilitate the automation of the various workflows that are part of every development
and testing processes.
    """
    main_parser = ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    sub_parser = main_parser.add_subparsers()

    create_list_sub_parser(sub_parser)
    create_info_sub_parser(sub_parser)

    return main_parser


def create_plugin_arg_parser()->ArgumentParser:
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
    return plugin_support_parser


def create_list_sub_parser(sub_parser):
    list_parser = sub_parser.add_parser("list", help="Lists FireX microservices, or used arguments"
                                                     "  {microservices,arguments}",
                                        parents=[create_plugin_arg_parser()])
    list_group = list_parser.add_mutually_exclusive_group(required=True)
    list_group.add_argument("--microservices", '-microservices', help="Lists all available microservices",
                            action='store_true')
    list_group.add_argument("--arguments", '-arguments', help="Lists all arguments used by microservices",
                            action='store_true')

    list_group.set_defaults(func=pick_list_func)

    return list_group


def pick_list_func(args):
    from firexapp.info import print_available_microservices, print_argument_used
    if args.microservices:
        print_available_microservices(args.plugins)
    elif args.arguments:
        print_argument_used(args.plugins)


def create_info_sub_parser(sub_parser):
    info_parser = sub_parser.add_parser("info", help="Lists detailed information about a microservice",
                                        parents=[create_plugin_arg_parser()])
    info_parser.add_argument("entity", help="The short or long name of the microservice to be detailed, or a "
                                            "microservice argument")

    info_parser.set_defaults(func=get_info_func)
    return info_parser


def get_info_func(args):
    from firexapp.info import print_details
    print_details(args.entity, args.plugins)
    sys.exit(0)
