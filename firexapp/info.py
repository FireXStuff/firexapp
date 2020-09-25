import inspect
import re
from textwrap import wrap
from celery.exceptions import NotRegistered
from firexapp.plugins import plugin_support_parser
from firexapp.application import import_microservices, get_app_task


class InfoBaseApp:
    def __init__(self):
        self._list_sub_parser = None
        self._info_sub_parser = None

    def create_list_sub_parser(self, sub_parser):
        list_parser = sub_parser.add_parser("list", help="Lists FireX microservices, or used arguments"
                                                         "  {microservices,arguments}",
                                            parents=[plugin_support_parser])
        list_group = list_parser.add_mutually_exclusive_group(required=True)
        list_group.add_argument("--microservices", '-microservices', help="Lists all available microservices",
                                action='store_true')
        list_group.add_argument("--arguments", '-arguments', help="Lists all arguments used by microservices",
                                action='store_true')

        list_group.set_defaults(func=self.run_list)

        return list_group

    def create_info_sub_parser(self, sub_parser):
        if not self._info_sub_parser:

            info_parser = sub_parser.add_parser("info", help="Lists detailed information about a microservice",
                                                parents=[plugin_support_parser])
            info_parser.add_argument("entity", help="The short or long name of the microservice to be detailed, or a "
                                                    "microservice argument. It can be a Python compatible regexp to "
                                                    "display information about all services matching that expression.")

            info_parser.set_defaults(func=self.run_info)
            self._info_sub_parser = info_parser
        return self._info_sub_parser

    def run_list(self, args):
        if args.microservices:
            self.print_available_microservices(args.plugins)
        elif args.arguments:
            self.print_argument_used(args.plugins)

    def run_info(self, args):
        self.print_details(args.entity, args.plugins)

    @staticmethod
    def print_available_microservices(plugins: str):
        apps = import_microservices(plugins)
        print()
        print("The following microservices are available:")

        services = [str(task) for task in apps]
        services = [task for task in services if not task.startswith('celery.')]  # filter out base celery types
        services.sort()
        for service in services:
            print(service)

        pointers = [(full, apps[full].name) for full in apps if apps[full].name not in full]
        if pointers:
            print("\nPointers (override -> original):")
            for new, old in pointers:
                print(new, "->", old)
        print("\nUse the info sub-command for more details\n")

    @staticmethod
    def print_argument_used(plugins: str):
        all_tasks = import_microservices(plugins)
        print()
        print("The following arguments are used by microservices:")
        usage = get_argument_use(all_tasks)
        for arg in sorted(usage):
            print(arg)
        print("\nUse the info sub-command for more details\n")

    def print_details(self, entity, plugins, all_tasks=None):
        if not all_tasks:
            all_tasks = import_microservices(plugins)

        entries_found = 0
        # Is this thing a microservice?
        for task_name in sorted(all_tasks, key=lambda i: i.split('.')[-1]):

            if not re.search(entity, task_name):
                continue

            # noinspection PyUnusedLocal
            task = None
            try:
                task = get_app_task(task_name, all_tasks)
            except NotRegistered:
                continue

            if task:
                if entries_found > 0:
                    print('\n')
                self.print_task_details(task)
                entries_found += 1

        if entries_found > 0:
            return

        # Is this thing an argument
        all_args = get_argument_use(all_tasks)
        if entity in all_args:
            print("Argument name: " + entity)
            print("Used in the following microservices:")
            for micro in all_args[entity]:
                print(micro.name)
            return

        self._info_sub_parser.exit(status=-1, message="Microservice %s was not found!" % entity)

    @classmethod
    def parse_task_docstring(cls, task):
        if not task.__doc__:
            return None, None

        header = None
        arg_dict = None
        docstring = inspect.getdoc(task)

        match = re.search(r"^(.*)\n\s*Arguments?[^\n]*\n\s*-*(.*)", docstring, re.MULTILINE | re.DOTALL)
        if match:
            if len(match.group(1).strip()):
                header = match.group(1).strip()
            if len(match.group(2).strip()):
                arg_desc_str = match.group(2).strip()
                arg_dict = {}

                # Need to determine if args are indicated with prefixed '--'
                if re.search("^\s*--", arg_desc_str):
                    # assume our args all start with --
                    arg_prefix = "(?:--)"
                else:
                    # no prefix
                    arg_prefix = ""
                # Go over arguments section and create dict of args/description
                desc = ''
                arg = None
                for line in arg_desc_str.split("\n"):
                    line = line.strip()
                    # blank line ends entry
                    if not len(line):
                        if arg:
                            arg_dict[arg] = desc
                        arg = None
                        desc = ''
                    # Look for start of new entry
                    match = re.search(r"^" + arg_prefix + r"(\S[^:\(\r\n]+)(?:\([^)\r\n]*\))?:(?:(?:\([^)\r\n]*\))?[^\S\r\n]*([^\r\n]*))?$", line)
                    if match:
                        if arg:
                            arg_dict[arg] = desc
                        arg = match.group(1)
                        desc = match.group(2).strip()
                        # Remove ':' and any leading () that usually indicate type. These can appear in either order
                        match = re.search(r"^(?:\([^)]*\)\s*)?:(?:\([^)]*\))?\s*(.+)$", desc)
                        if match:
                            desc = match.group(1)
                    else:
                        # continue prev entry
                        if len(desc):
                            desc += " " + line
                        else:
                            desc = line
                # store last outstanding entry
                if arg:
                    arg_dict[arg] = desc
        else:
            header = docstring
        return header, arg_dict

    @classmethod
    def print_task_details(cls, task):
        dash_length = 40
        print('-' * dash_length)
        split_name = task.name.split(".")
        name = split_name[-1]
        path = '.'.join(split_name[0:-1])
        if path:
            path = " (%s)" % path
        print("Name: " + name + path)

        header, arguments = cls.parse_task_docstring(task)

        if header:
            print('\n' + header)
        if not arguments:
            arguments = {}

        def print_arg(arg, deflt, description):
            max_arg_len = 25
            max_desc_len = 80 - max_arg_len - len(tab)
            arg_str = f"{tab}{arg}"

            # Add default value, if present
            if deflt is not None:
                arg_str += f"(default={deflt})"

            if description:
                # Add filler or newline, depending on arg_str length
                if len(arg_str) >= max_arg_len:
                    arg_str += "\n" + (' ' * max_arg_len)
                else:
                    arg_str += " " * (max_arg_len - len(arg_str))

                # Remove excess spacing in description
                description = re.sub(r" {2,}", " ", description)

                # Add description
                if len(description) < max_desc_len:
                    arg_str += description
                else:
                    arg_str += f"\n{' ' * max_arg_len}".join(wrap(description, max_desc_len))
            print(arg_str)

        tab = ' ' * 1
        print("\nArguments info")
        print("--------------")
        required_args = getattr(task, "required_args", [])
        cnt = 0
        for chain_arg in sorted(required_args):
            if "self" not in chain_arg and \
               "uid" not in chain_arg and \
               chain_arg is not 'kwargs':
                desc = arguments.get(chain_arg, None)
                print_arg(chain_arg, None, desc)
                cnt += 1

        optional_args = getattr(task, "optional_args", {})
        if len(optional_args):
            for chain_arg in sorted(optional_args):
                if chain_arg.startswith('_'):
                    continue
                desc = arguments.get(chain_arg, None)
                default = optional_args[chain_arg]
                if default is None:
                    default = "None"
                print_arg(chain_arg, default, desc)
        else:
            if not cnt:
                print(tab, "None")

        print('\nReturns\n-------')
        out = getattr(task, "return_keys", {})
        if out:
            for chain_arg in sorted(out):
                desc = arguments.get(chain_arg, None)
                print_arg(chain_arg, None, desc)
        else:
            print(tab, "None")


def get_argument_use(all_tasks) -> dict:
    argument_usage = {}
    for _, task in all_tasks.items():
        if not hasattr(task, "required_args") or not hasattr(task, "optional_args"):
            continue

        for arg in task.required_args + list(task.optional_args):
            if arg in argument_usage:
                argument_usage[arg].add(task)
            else:
                tasks = set()
                tasks.add(task)
                argument_usage[arg] = tasks

    return argument_usage
