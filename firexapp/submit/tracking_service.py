from abc import ABC, abstractmethod
from typing import Optional

from firexapp.discovery import get_firex_tracking_services_entry_points, PkgVersionInfo
from firexapp.submit.install_configs import FireXInstallConfigs

_services = None


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

    def get_pkg_version_info(self) -> Optional[PkgVersionInfo]:
        return None


def get_service_name(service: TrackingService) -> str:
    return service.__class__.__name__


def get_tracking_services() -> Optional[tuple[TrackingService]]:
    global _services
    if _services is None:
        entry_pts = get_firex_tracking_services_entry_points()
        entry_objects = [e.load() for e in entry_pts]
        _services = tuple([point() for point in entry_objects])
    return _services

def get_tracking_services_versions() -> list[PkgVersionInfo]:
    version_infos = [service.get_pkg_version_info() for service in get_tracking_services()]
    return [v for v in version_infos if v]

def has_flame() -> bool:
    # Unfortunate coupling, but just too many things vary depending on presence of flame. Will eventually bring
    # flame in to firexapp.
    return 'FlameLauncher' in get_tracking_services()