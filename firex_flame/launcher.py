import os
import subprocess
import time
import distutils.util
import socket
import time

from firexapp.common import qualify_firex_bin, select_env_vars
from firexapp.submit.submit import OptionalBoolean
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.console import setup_console_logging
from firexapp.submit.install_configs import FireXInstallConfigs

from firex_flame.flame_helper import DEFAULT_FLAME_TIMEOUT, get_flame_debug_dir, get_rec_file, is_json_file, \
    get_flame_redirect_file_path
from firex_flame.model_dumper import is_dump_complete, get_run_metadata_file, wait_and_get_flame_url
from firexapp.discovery import PkgVersionInfo

logger = setup_console_logging(__name__)


def _send_flame_indicate_ready(celery_app):
    if celery_app:
        with celery_app.events.default_dispatcher(hostname=socket.gethostname()) as d:
            # Flame will indicate it's ready for tasks once it gets this Celery event.
            return d.send('flame-indicate-ready')
    else:
        logger.warning(
            "Flame launcher did not receive Celery app, "
            "cannot send event for server to indicate readiness.")


def get_flame_args(uid, broker_recv_ready_file, args):
    if args.flame_record:
        rec_file = args.flame_record
    else:
        rec_file = get_rec_file(uid.logs_dir)

    # assemble startup cmd
    cmd_args = {
        'port': args.flame_port,
        'uid': str(uid),
        'logs_dir': uid.logs_dir,
        'chain': args.chain,
        'recording': rec_file,
        'central_server': args.flame_central_server,
        'central_server_ui_path': args.flame_central_server_ui_path,
        'logs_server': args.flame_logs_server,
        'central_documentation_url': args.flame_central_documentation_url,
        'flame_timeout': args.flame_timeout,
        'broker_recv_ready_file': broker_recv_ready_file,
        'broker_max_retry_attempts': args.broker_max_retry_attempts,
        'terminate_on_complete': args.flame_terminate_on_complete,
        'firex_bin_path': args.firex_bin_path,
        'extra_task_dump_paths': args.flame_extra_task_dump_paths,
        'serve_logs_dir': args.flame_serve_logs_dir,
        'authed_user_request_path': args.flame_authed_user_request_path,
        'wait_for_webserver': args.flame_wait_for_webserver,
    }
    result = []
    for k, v in cmd_args.items():
        if v is not None:
            result.append(f'--{k}')
            result.append('%s' % v)
    return result


class FlameLauncher(TrackingService):

    def __init__(self):
        self.broker_recv_ready_file = None
        self.sync = None
        self.firex_logs_dir = None
        self.is_ready_for_tasks = False
        self.start_time = None
        self.stdout_file = None
        self.wait_for_webserver = None

    def extra_cli_arguments(self, arg_parser):
        arg_parser.add_argument('--flame_timeout', help='How long the webserver should run for, in seconds.',
                                default=DEFAULT_FLAME_TIMEOUT)
        arg_parser.add_argument('--flame_central_server',
                                help='Server URL from which flame resources can be fetched to enable browser caching'
                                     'and client-side settings.',
                                default=None)
        arg_parser.add_argument('--flame_central_server_ui_path',
                                help='Path relative to flame_central_server from which the Flame UI is served.',
                                default=None)
        arg_parser.add_argument('--flame_logs_server',
                                help='Server URL from which flame logs can be fetched.',
                                default=None)
        arg_parser.add_argument('--flame_central_documentation_url',
                                help='URL linking to main out-of-app docs.',
                                default=None)
        arg_parser.add_argument('--firex_bin_path',
                                help='Path to firex executable.',
                                default=None)
        arg_parser.add_argument('--broker_max_retry_attempts',
                                help='See Flame argument help.',
                                default=None)
        arg_parser.add_argument('--flame_record',
                                help='A file to record flame events',
                                default=None)
        arg_parser.add_argument('--flame_port',
                                help='Flame port to be used', type=int,
                                default=0)
        arg_parser.add_argument('--flame_terminate_on_complete',
                                help='Terminate Flame when run completes. Ignores timeout arg entirely.',
                                default=None, const=True, nargs='?', action=OptionalBoolean)
        arg_parser.add_argument('--flame_wait_for_webserver',
                                help='Wait for webserver when waiting to be ready for tasks.',
                                default=True, const=True, nargs='?', action=OptionalBoolean)
        arg_parser.add_argument('--flame_extra_task_dump_paths',
                                help='Paths specifying alternative task represetnation to dump at end of flame.',
                                default=None)
        arg_parser.add_argument('--flame_authed_user_request_path', default=None)
        arg_parser.add_argument('--flame_serve_logs_dir',
                                help="Control if the Flame server makes the run's logs_dir available via HTTP(S).",
                                type=lambda x: bool(distutils.util.strtobool(x)),
                                default=None)


    def start(self, args, install_configs: FireXInstallConfigs, uid=None, **kwargs) -> dict:
        super().start(args, install_configs, uid=uid, **kwargs)

        flame_debug_dir = get_flame_debug_dir(uid.logs_dir)
        os.makedirs(flame_debug_dir, exist_ok=True)
        self.broker_recv_ready_file = os.path.join(flame_debug_dir, 'celery_receiver_ready')

        self.sync = args.sync
        self.firex_logs_dir = uid.logs_dir

        self.wait_for_webserver = args.flame_wait_for_webserver

        flame_args = get_flame_args(uid, self.broker_recv_ready_file, args)
        self.stdout_file = os.path.join(flame_debug_dir, 'flame.stdout')

        self.start_time = time.time()
        try:
            with open(self.stdout_file, 'w+') as f:
                subprocess.Popen([qualify_firex_bin("firex_flame")] + flame_args,
                                 stdout=f, stderr=subprocess.STDOUT,
                                 close_fds=True,
                                 env=select_env_vars(['PATH']),
                                 preexec_fn=os.setpgrp, # Avoid SIGINTs sent to FireX by having own process group.
                                 cwd=flame_debug_dir,
                                 )
        except Exception as e:
            logger.error("Flame subprocess start failed: %s." % e)
            raise

        return {}

    def ready_for_tasks(self, celery_app=None, **kwargs) -> bool:
        if not self.is_ready_for_tasks:
            # Flame will receive this event once its receiving from Celery
            # and create broker_recv_ready_file
            _send_flame_indicate_ready(celery_app)
            time.sleep(0.1)
            broker_ready = os.path.isfile(self.broker_recv_ready_file)

            if self.wait_for_webserver:
                run_metadata_file = get_run_metadata_file(firex_logs_dir=self.firex_logs_dir)
                webserver_ready = os.path.isfile(run_metadata_file) and is_json_file(run_metadata_file)
            else:
                webserver_ready = True

            self.is_ready_for_tasks = broker_ready and webserver_ready
            if self.is_ready_for_tasks:
                logger.debug("Flame up after %.2f s" % (time.time() - self.start_time))
                if self.wait_for_webserver:
                    # Only print Flame URL if we've waited for webserver, since otherwise we don't know the port.
                    logger.info(f"Flame: {self.get_viewer_url()}")
                self.write_flame_redirect()

        return self.is_ready_for_tasks

    def write_flame_redirect(self):
        # Write the flame redirect file in the run's logs dir
        flame_redirect_filepath = get_flame_redirect_file_path(self.firex_logs_dir)
        with open(flame_redirect_filepath, 'w') as f:
            f.write(f'<meta http-equiv="refresh" content="0; url={self.get_viewer_url()}" />')

    def ready_release_console(self, **kwargs) -> bool:
        if self.sync:
            # For sync requests, guarantee that the model is completely dumped before terminating.
            return is_dump_complete(self.firex_logs_dir)
        return True

    def get_viewer_url(self):
        if (
            not self.install_configs.has_viewer()
            and self.firex_logs_dir
        ):
            return wait_and_get_flame_url(self.firex_logs_dir, timeout=0)
        return self.install_configs.run_url

    @staticmethod
    def get_pkg_version_info() -> PkgVersionInfo:
        import firex_flame
        return PkgVersionInfo(pkg='firex-flame',
                              version=firex_flame.__version__,
                              commit=firex_flame._version.get_versions()['full-revisionid'])