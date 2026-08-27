import os
from psutil import Process, TimeoutExpired
import subprocess

from firexapp.submit.tracking_service import TrackingService
from firexapp.common import qualify_firex_bin
from firexapp.submit.console import setup_console_logging

from firex_keeper.keeper_helper import get_keeper_dir
from firexapp.discovery import PkgVersionInfo

logger = setup_console_logging(__name__)


class FireXKeeperLauncher(TrackingService):

    def __init__(self):
        self.broker_recv_ready_file = None

    def start(self, args, uid=None, **kwargs)->{}:
        keeper_debug_dir = get_keeper_dir(uid.logs_dir)
        os.makedirs(keeper_debug_dir, exist_ok=True)
        self.broker_recv_ready_file = os.path.join(keeper_debug_dir, 'keeper_celery_recvr_ready')
        stdout_file = os.path.join(keeper_debug_dir, 'keeper.stdout.txt')

        cmd = [qualify_firex_bin("firex_keeper"),
               "--uid", str(uid),
               "--logs_dir", uid.logs_dir,
               "--chain", args.chain,
               "--broker_recv_ready_file", self.broker_recv_ready_file,
               ]
        with open(stdout_file, 'w+') as f:
            pid = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                close_fds=True,
                cwd=keeper_debug_dir,
            ).pid

        try:
            Process(pid).wait(0.1)
        except TimeoutExpired:
            logger.debug("Started background FireXKeeper with pid %s" % pid)
        else:
            logger.error("Failed to start FireXKeeper -- task DB will not be available.")

        return {}

    def ready_for_tasks(self, **kwargs) -> bool:
        return os.path.isfile(self.broker_recv_ready_file)

    @staticmethod
    def get_pkg_version_info() -> PkgVersionInfo:
        import firex_keeper
        return PkgVersionInfo(pkg='firex-keeper',
                              version=firex_keeper.__version__,
                              commit=firex_keeper._version.get_versions()['full-revisionid'])
