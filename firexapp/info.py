import inspect
from celery.exceptions import NotRegistered
from firexapp.application import import_microservices, get_app_task


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


def print_argument_used(plugins: str):
    all_tasks = import_microservices(plugins)
    print()
    print("The following arguments are used by microservices:")
    usage = get_argument_use(all_tasks)
    for arg in sorted(usage):
        print(arg)
    print("\nUse the info sub-command for more details\n")


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


def print_details(entity, plugins, all_tasks=None):
    if not all_tasks:
        all_tasks = import_microservices(plugins)

    # Is this thing a microsevice?
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
        print_task_details(task)
        return

    # Is this thing an argument
    all_args = get_argument_use(all_tasks)
    if entity in all_args:
        print("Argument name: " + entity)
        print("Used in the following microservices:")
        for micro in all_args[entity]:
            print(micro.name)
        return

    print("Microservice %s was not found!" % entity)


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

