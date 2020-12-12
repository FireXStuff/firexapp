from abc import ABC, abstractmethod
from collections import OrderedDict
from firexapp.submit.install_configs import FireXInstallConfigs

TRACKING_SERVICE_ENTRY_POINT = 'firex_tracking_service'

_services = None


#
# In case there are duplicate modules found, only keep one for each
#   (name, module_name, object_name) tuple. This prevents duplicate
#   arg registration failures when the sys.path causes the same service
#   to be found twice.
#
def prune_duplicate_module_entry_points(entry_points):
    id_to_entry_points = OrderedDict()

    for e in entry_points:
        key = (e.name, e.module_name, e.object_name)
        if key not in id_to_entry_points:
            id_to_entry_points[key] = e
        # Replace the currently stored entry point for this key if the distro is None.
        elif id_to_entry_points[key].distro is None and e.distro is not None:
            id_to_entry_points[key] = e

    return list(id_to_entry_points.values())


def get_tracking_services() -> ():
    global _services
    if _services is None:
        import entrypoints
        entry_pts = [entry_point for entry_point in entrypoints.get_group_all(TRACKING_SERVICE_ENTRY_POINT)]
        entry_pts = prune_duplicate_module_entry_points(entry_pts)
        entry_objects = [e.load() for e in entry_pts]
        _services = tuple([point() for point in entry_objects])
    return _services


def has_flame():
    # Unfortunate coupling, but just too many things vary depending on presence of flame. Will eventually bring
    # flame in to firexapp.
    return 'FlameLauncher' in get_tracking_services()


class TrackingService(ABC):

    install_configs: FireXInstallConfigs

    def extra_cli_arguments(self, arg_parser):
        pass

    @abstractmethod
    def start(self, args, install_configs: FireXInstallConfigs, **kwargs) -> {}:
        self.install_configs = install_configs

    def ready_for_tasks(self, **kwargs) -> bool:
        return True

    def ready_release_console(self, **kwargs) -> bool:
        return True


def get_service_name(service: TrackingService) -> str:
    return service.__class__.__name__
