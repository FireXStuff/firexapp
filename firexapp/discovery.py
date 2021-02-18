import os
import sys
import logging

TASKS_DIRECTORY = "firex_tasks_directory"


logger = logging.getLogger(__name__)


def _get_paths_without_cwd():
    # This is needed because Celery temporarily adds the cwd into the sys.path via a context switcher,
    # and our discovery takes place inside that context.
    # Having cwd in the sys.path can slow down the discovery significantly without any benefit.
    paths = list(sys.path)
    try:
        paths.remove(os.getcwd())
    except ValueError:  # pragma: no cover
        pass
    return paths


def _get_firex_dependant_package_locations() -> []:
    import entrypoints
    locations = []
    eps = [ep for ep in entrypoints.get_group_all('firex.bundles')]
    for ep in eps:
        loaded_pkg = ep.load()
        locations.extend(loaded_pkg.__path__)
    return locations


def discover_package_modules(current_path, root_path=None) -> []:
    if root_path is None:
        root_path = os.path.dirname(current_path)

    services = []
    if os.path.isfile(current_path):
        basename, ext = os.path.splitext(current_path)
        if ext.lower() == ".py" and not os.path.basename(current_path).startswith('_'):
            basename = basename.replace(root_path, "")
            return [basename.replace(os.path.sep, ".").strip(".")]
        else:
            return []
    elif os.path.isdir(current_path):
        base = os.path.basename(current_path)
        if "__pycache__" in base or base.startswith("."):
            return []
        for child_name in os.listdir(current_path):
            full_child = os.path.join(current_path, child_name)
            services += discover_package_modules(full_child, root_path)
        return services
    else:
        # either a symlink or a path that doesn't exist
        return []


def find_firex_task_bundles()->[]:
    # look for task modules in dependant packages
    bundles = []
    for location in _get_firex_dependant_package_locations():
        bundles += discover_package_modules(location)
    # look for task modules in env defined location
    if TASKS_DIRECTORY in os.environ:
        include_location = os.environ[TASKS_DIRECTORY]
        if os.path.isdir(include_location):
            if include_location not in sys.path:
                sys.path.append(include_location)
            include_tasks = discover_package_modules(include_location, root_path=include_location)
            bundles += include_tasks

    return bundles
