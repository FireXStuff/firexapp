import os
import sys
import logging
from collections import OrderedDict, namedtuple
from typing import Dict, List, Tuple

from entrypoints import EntryPoint

TASKS_DIRECTORY = "firex_tasks_directory"

logger = logging.getLogger(__name__)

_loaded_firex_bundles = {}


class PkgVersionInfo(namedtuple('PkgVersionInfo', ('pkg', 'version', 'commit'), defaults=(None, None, None))):
    def __str__(self):
        return f'{self.pkg}: {self.version or self.commit}'


def _get_paths_without_cwd() -> [str]:
    # This is needed because Celery temporarily adds the cwd into the sys.path via a context switcher,
    # and our discovery takes place inside that context.
    # Having cwd in the sys.path can slow down the discovery significantly without any benefit.
    paths = list(sys.path)
    try:
        paths.remove(os.getcwd())
    except ValueError:  # pragma: no cover
        pass
    return paths


#
# In case there are duplicate modules found, only keep one for each
#   (name, module_name, object_name) tuple. This prevents duplicate
#   arg registration failures when the sys.path causes the same service
#   to be found twice.
#
def prune_duplicate_module_entry_points(entry_points) -> [EntryPoint]:
    id_to_entry_points = OrderedDict()

    for e in entry_points:
        key = (e.name, e.module_name, e.object_name)
        if key not in id_to_entry_points:
            id_to_entry_points[key] = e
        # Replace the currently stored entry point for this key if the distro is None.
        elif id_to_entry_points[key].distro is None and e.distro is not None:
            id_to_entry_points[key] = e

    return list(id_to_entry_points.values())


def _get_entrypoints(name, prune_duplicates=True, path=None) -> [EntryPoint]:
    import entrypoints
    if path is not None and not isinstance(path, list):
        path = [path]
    eps = [ep for ep in entrypoints.get_group_all(name, path=path)]
    if prune_duplicates:
        eps = prune_duplicate_module_entry_points(eps)
    return eps


def loaded_firex_core_entry_points(path=None) -> Dict[EntryPoint, object]:
    return _load_firex_entry_points('firex.core', path=path)


def loaded_firex_bundles_entry_points(path=None) -> Dict[EntryPoint, object]:
    return _load_firex_entry_points('firex.bundles', path=path)


def loaded_firex_entry_points(path=None):
    cores = loaded_firex_core_entry_points(path=path)
    bundles = loaded_firex_bundles_entry_points(path=path)
    return {**cores, **bundles}

def _load_firex_entry_points(entrypoint_name, path=None) -> Dict[EntryPoint, object]:
    global _loaded_firex_bundles
    key = str(path)
    try:
        return _loaded_firex_bundles[key][entrypoint_name]
    except KeyError:
        eps = _get_entrypoints(entrypoint_name, path=path)
        loaded_eps = {ep: ep.load() for ep in eps}
        try:
            _loaded_firex_bundles[key][entrypoint_name] = loaded_eps
        except KeyError:
            _loaded_firex_bundles[key] = dict(entrypoint_name=loaded_eps)
        return loaded_eps


def get_firex_tracking_services_entry_points() -> [EntryPoint]:
    return _get_entrypoints('firex_tracking_service')


def get_firex_dependant_package_versions() -> [PkgVersionInfo]:
    versions = list()
    for ep, loaded_pkg in loaded_firex_entry_points().items():
        try:
            version = loaded_pkg.__version__
        except AttributeError:
            version = None
        try:
            commit = loaded_pkg._version.get_versions()['full-revisionid']
        except AttributeError:
            commit = None
        versions.append(PkgVersionInfo(pkg=ep.name, version=version, commit=commit))
    return versions


def get_all_pkg_versions() -> [PkgVersionInfo]:
    from firexapp.submit.tracking_service import get_tracking_services_versions
    return get_tracking_services_versions() + get_firex_dependant_package_versions()


def get_all_pkg_versions_as_dict() -> dict:
    return {pkg_info.pkg: pkg_info for pkg_info in get_all_pkg_versions()}


def get_all_pkg_versions_str() -> str:
    pkg_version_info_str = [f'\t - {p_info}' for p_info in get_all_pkg_versions()]
    return 'FireX Package Versions:\n' + '\n'.join(pkg_version_info_str) + '\n'


def _find_bundle_pkg_root(path, namespace):
    while True:
        head, tail = os.path.split(path)
        if tail == namespace:
            return head
        else:
            path = os.path.dirname(path)


# Return a list of two-element tuples
# Where the 1st element is the path of package
# and the 2nd element is the path of package's root
def _get_firex_bundle_package_locations(path=None) -> List[Tuple[str, str]]:
    locations = []
    loaded_entry_points = loaded_firex_bundles_entry_points(path=path)
    for p in loaded_entry_points.values():
        namespace = p.__package__.split('.')[0]
        pkg_paths = p.__path__
        for pkg_path in pkg_paths:
            root = _find_bundle_pkg_root(pkg_path, namespace)
            locations.append((pkg_path, root))
    return locations


def discover_package_modules(current_path, root_path=None) -> [str]:
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


def find_firex_task_bundles() -> [str]:
    # look for task modules in dependant packages
    bundles = []
    locations = _get_firex_bundle_package_locations()
    for path, root_path in locations:
        bundles += discover_package_modules(path, root_path)
    # look for task modules in env defined location
    if TASKS_DIRECTORY in os.environ:
        include_location = os.environ[TASKS_DIRECTORY]
        if os.path.isdir(include_location):
            if include_location not in sys.path:
                sys.path.append(include_location)
            include_tasks = discover_package_modules(include_location, root_path=include_location)
            bundles += include_tasks

    return bundles
