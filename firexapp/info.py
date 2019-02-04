import inspect
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
                                                    "microservice argument")

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

        # Is this thing a microservice?
        task = None
        try:
            task = get_app_task(entity, all_tasks)
        except NotRegistered:
            if "." in entity:
                try:
                    task = get_app_task(entity.split('.')[-1], all_tasks)
                except NotRegistered:
                    pass
        if task:
            self.print_task_details(task)
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

    @staticmethod
    def print_task_details(task):
        dash_length = 40
        print('-' * dash_length)
        print("Short Name:", task.name.split(".")[-1])
        print('Full Name: %s' % task.name)

        if task.__doc__:
            print('-' * dash_length)
            print(inspect.getdoc(task))

        required_args = getattr(task, "required_args", [])
        if len(required_args):
            print('-' * dash_length)
            print("Mandatory arguments:")
            for chain_arg in required_args:
                if "self" not in chain_arg and \
                   "uid" not in chain_arg and \
                   chain_arg is not 'kwargs':
                    print("\t", chain_arg)

        optional_args = getattr(task, "optional_args", {})
        if len(optional_args):
            print('-' * dash_length)
            print("Optional arguments:")
            for chain_arg in optional_args:
                print("\t", chain_arg + "=" + str(optional_args[chain_arg]))

        out = getattr(task, "return_keys", {})
        if out:
            print('-' * dash_length)
            print('Returns:')
            for chain_arg in out:
                print("\t", chain_arg)
        print('-' * dash_length)


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
