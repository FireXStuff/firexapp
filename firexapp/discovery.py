import os
import sys
import logging
from distlib.database import DistributionPath


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


def _get_firex_dependant_package_locations()-> []:
    distributions = DistributionPath(path=_get_paths_without_cwd(), include_egg=True).get_distributions()

    # some packages (such as any tree) might cause exceptions in logging
    old_raise = logging.raiseExceptions
    try:
        logging.raiseExceptions = False
        firex_app_name = __name__.split(".")[0]
        logging.getLogger('distlib.metadata').setLevel(logging.WARNING)
        logging.getLogger('distlib.database').setLevel(logging.WARNING)
        dependants = [d for d in distributions if firex_app_name in d.run_requires]
    finally:
        logging.raiseExceptions = old_raise

    locations = []
    for d in dependants:
        top = os.path.join(d.path, "top_level.txt")
        with open(top) as t:
            top_package = t.read().strip()
            package_location = os.path.join(os.path.dirname(d.path), top_package)
            locations.append(package_location)
    return locations


def discover_package_modules(current_path, root_path=None) -> []:
    if root_path is None:
        root_path = os.path.dirname(current_path)

    services = []
    if os.path.isfile(current_path):
        basename, ext = os.path.splitext(current_path)
        if ext.lower() == ".py":
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
    bundles = []
    for location in _get_firex_dependant_package_locations():
        bundles += discover_package_modules(location)
    return bundles
