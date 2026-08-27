import sys

noop_class = type('noop', (object,), {'iter_entry_points': lambda _: []})
sys.modules["pkg_resources"] = noop_class
sys.modules["celery.events.dispatcher"] = type('noop2', (object,), {'EventDispatcher': noop_class})

from gevent import monkey
monkey.patch_all()

import argparse
import distutils.util
import logging
import os
from pathlib import Path
from gevent import signal
from threading import Timer

from firex_flame.main_app import start_flame
from firex_flame.flame_helper import get_flame_debug_dir, get_flame_pid_file_path, DEFAULT_FLAME_TIMEOUT, \
    BrokerConsumerConfig, get_flame_url_from_port, wait_until, FlameServerConfig, REVOKE_REASON_KEY
# Prevent dependencies from taking module loading hit of pkg_resources.
from firexapp.submit.submit import OptionalBoolean

logger = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', help='Port for starting the web server on', type=int, default=0)
    parser.add_argument('--uid', help='Unique identifier for the represented FireX run.')
    parser.add_argument('--logs_dir', help='Logs directory.', default=None, required=True)
    parser.add_argument('--chain', help='Chain of the run.', default=None)
    # TODO: could validate either rec file exists or broker URL is legible from logs_dir is supplied.
    parser.add_argument('--recording', help='A file containing the recording of celery events.', default=None)
    parser.add_argument('--central_server', help='A central web server from which the UI and logs can be served.',
                        default=None)
    parser.add_argument('--central_server_ui_path', help='Path part of the URL from which the central server serves'
                                                         'the UI. Only has meaning when a central_server is supplied.',
                        default=None)
    parser.add_argument('--logs_server', help='A central web server from which logs can be fetched.',
                        default=None)
    parser.add_argument('--central_documentation_url', help='URL linking to main out-of-app docs.',
                        default='http://www.firexapp.com/')
    parser.add_argument('--firex_bin_path', help='Path to a firex executable.',
                        default=None)
    parser.add_argument('--flame_timeout', help='Maximum lifetime of this service, in seconds', type=int,
                        default=DEFAULT_FLAME_TIMEOUT)
    parser.add_argument('--broker_recv_ready_file', help='File to create immediately before capturing celery events.',
                        default=None)
    parser.add_argument('--broker_max_retry_attempts',
                        help='Number of retry attempts if connection with broker is lost. '
                             'Retries are backed-off exponentially with base 2,'
                             'so a value of 3 here with cause sleeps between retries of '
                             '1 sec, 2 sec, 4 sec, before giving up on retries. Default waits at least a total of '
                             '63 seconds before giving up on retries.',
                        type=int,
                        default=5)
    parser.add_argument('--terminate_on_complete',
                        help='Supply if the Flame server should terminate when the run completes. '
                             'Causes the value of --flame_timeout to be ignored entirely.',
                        default=None, const=True, nargs='?', action=OptionalBoolean)
    parser.add_argument('--extra_task_dump_paths',
                        help='Path to files specifying alternate task dump formats.',
                        type=str, default='')
    parser.add_argument('--authed_user_request_path',
                        help='Dot-delimited path specifying where the web server can find the authed user.',
                        type=str, default='')
    parser.add_argument('--serve_logs_dir',
                        help='Whether or not the Flame web server should make the logs_dir of the run available via '
                             'HTTP(S).',
                        type=lambda x: bool(distutils.util.strtobool(x)),
                        default=True)
    parser.add_argument('--wait_for_webserver',
                        type=lambda x: bool(distutils.util.strtobool(x)),
                        default=True)

    return parser.parse_args()


class ShutdownHandler:

    def __init__(self):
        self.shutdown_received = False
        self.web_server = None
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)
        signal.signal(signal.SIGHUP,
                      lambda _, __: logger.warning("Ignoring SIGHUP signal -- will timeout with broker."))

    def sigterm_handler(self, _, __):
        self.shutdown('SIGTERM detected')

    def sigint_handler(self, _, __):
        self.shutdown('SIGINT detected')

    def timeout_handler(self):
        self.shutdown('timeout exceeded')

    def shutdown(self, reason):
        self.shutdown_received = True
        logging.info("Stopping entire Flame Server for reason: %s" % reason)

        # Avoid race condition where webserver is started immediately after shutdown is called.
        wait_until(lambda: self.web_server is not None, timeout=5, sleep_for=0.5)
        if self.web_server:
            self.web_server.stop()

        # lazy load this module for startup performance.
        from firex_flame.api import term_all_subprocs
        term_all_subprocs()
        logging.shutdown()


def _config_logging(root_logs_dir):
    flame_logs_dir = get_flame_debug_dir(root_logs_dir)
    os.makedirs(flame_logs_dir, exist_ok=True)

    Path(get_flame_pid_file_path(root_logs_dir)).write_text(str(os.getpid()))

    logging.basicConfig(
        filename=os.path.join(flame_logs_dir, 'flame.log'),
        format='[%(asctime)s][%(levelname)s][%(name)s]: %(message)s',
        level=logging.DEBUG,
    )
    # This module is very noisy (logs all data sent), so turn up the level.
    logging.getLogger('engineio.server').setLevel(logging.WARNING)
    logging.getLogger('geventwebsocket.handler').setLevel(logging.WARNING)


def _create_run_metadata(cli_args):
    #FIXME: should be a dataclass.
    return {
        'uid': cli_args.uid,
        'logs_dir': cli_args.logs_dir,
        'central_server': cli_args.central_server,
        'central_server_ui_path': cli_args.central_server_ui_path,
        'chain': cli_args.chain,
        'logs_server': cli_args.logs_server,
        'central_documentation_url': cli_args.central_documentation_url,
        # flame_url is lazy loaded since port is only guaranteed to be known after the web server has started.
        'flame_url': None,
        'firex_bin': cli_args.firex_bin_path,
        'root_uuid': None,
        REVOKE_REASON_KEY: None, # FIXME: delete entirely; moved to run.json
        'revoke_timestamp': None, # FIXME: delete entirely; moved to run.json
    }


def create_broker_processor_config(args):
    return BrokerConsumerConfig(args.broker_max_retry_attempts,
                                args.broker_recv_ready_file,
                                args.terminate_on_complete)


class NoopTimer:
    def start(self):
        pass

    def cancel(self):
        pass


def _create_server_config(args) -> FlameServerConfig:
    if args.extra_task_dump_paths:
        extra_task_dump_paths = args.extra_task_dump_paths.split(',')
    else:
        extra_task_dump_paths = []
    if args.authed_user_request_path:
        authed_user_request_path = args.authed_user_request_path.split('.')
    else:
        authed_user_request_path = []
    return FlameServerConfig(
        webapp_port=args.port,
        serve_logs_dir=args.serve_logs_dir,
        recording_file=args.recording,
        extra_task_dump_paths=extra_task_dump_paths,
        authed_user_request_path=authed_user_request_path
    )

def main():
    shutdown_handler = ShutdownHandler()

    args = _parse_args()
    _config_logging(args.logs_dir)
    t = NoopTimer() if args.terminate_on_complete else Timer(args.flame_timeout, shutdown_handler.timeout_handler)
    try:
        t.start()
        logger.info('Starting Flame Server with args: %s' % args)
        web_server = start_flame(
            _create_server_config(args),
            create_broker_processor_config(args),
            _create_run_metadata(args),
            shutdown_handler,
            args.wait_for_webserver,
        )
        # Allow the shutdown handler to stop the web server before we serve_forever.
        shutdown_handler.web_server = web_server
        print(f"Flame server running on: {get_flame_url_from_port(web_server.server_port)}")
        web_server.serve_forever()
    except Exception as e:
        logger.exception(e)
        shutdown_handler.shutdown(str(e))
    finally:
        t.cancel()
        logger.info("Flame server finished.")


if __name__ == '__main__':
    main()
