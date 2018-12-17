from firexapp.application import import_microservices


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

        for arg in task.required_args + task.optional_args:
            if arg in argument_usage:
                argument_usage[arg].add(task)
            else:
                tasks = set()
                tasks.add(task)
                argument_usage[arg] = tasks

    return argument_usage
