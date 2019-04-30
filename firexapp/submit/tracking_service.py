from abc import ABC, abstractmethod

TRACKING_SERVICE_ENTRY_POINT = 'firex_tracking_service'

_services = None


def get_tracking_services() -> ():
    global _services
    if _services is None:
        import pkg_resources
        entry_pts = [entry_point for entry_point in pkg_resources.iter_entry_points(TRACKING_SERVICE_ENTRY_POINT)]
        entry_objects = [e.load() for e in entry_pts]
        _services = tuple([point() for point in entry_objects])
    return _services


class TrackingService(ABC):

    def extra_cli_arguments(self, arg_parser):
        pass

    @abstractmethod
    def start(self, args, **kwargs)->{}:
        pass
