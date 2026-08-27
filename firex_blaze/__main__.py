import argparse
import logging
import logging.handlers
import os
import sys
import signal

# Prevent dependencies from taking module loading hit of pkg_resources.
sys.modules["pkg_resources"] = type('noop', (object,), {})


from firexapp.events.model import FireXRunMetadata

from firex_blaze.blaze_event_consumer import BlazeKafkaSenderThread
from firex_blaze.fast_blaze_helper import get_blaze_dir
from firex_blaze.blaze_helper import BlazeSenderConfig, get_blaze_events_file, celery_app_from_logs_dir

logger = logging.getLogger(__name__)


def _parse_blaze_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance_name", help="Name of the blaze instance")
    parser.add_argument("--logs_dir", help="Logs directory for the run to keep task data for.",
                        required=True)
    parser.add_argument("--uid", help="FireX UID for the run to keep task data for.",
                        required=True)
    parser.add_argument("--firex_requester", help="Requester username, if different than running user",
                        required=True)
    parser.add_argument('--broker_recv_ready_file', help='File to create immediately before capturing celery events.',
                        default=None)
    parser.add_argument('--logs_url', help='Webserver used from which logs can be accessed.',
                        default=None)

    parser.add_argument('--kafka_topic', help='Topic use for the Kafka bus', required=True)
    parser.add_argument('--bootstrap_servers', help='Comma seperated list of Kafka bootrap servers.', required=True)
    parser.add_argument('--security_protocol', help='Protocol used to communicate with brokers. '
                                                    'Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL. '
                                                    'Default: PLAINTEXT.',
                        default='PLAINTEXT')

    # SASL-SSL OAuth 2.0 arguments
    parser.add_argument('--sasl_mechanism', help='SASL mechanism (e.g., OAUTHBEARER).',
                        default=None)
    parser.add_argument('--sasl_oauthbearer_method', help='OAuth bearer method (e.g., oidc).',
                        default=None)
    parser.add_argument('--sasl_oauthbearer_client_id', help='OAuth client ID.',
                        default=None)
    parser.add_argument('--sasl_oauthbearer_client_secret', help='OAuth client secret.',
                        default=None)
    parser.add_argument('--sasl_oauthbearer_token_endpoint_url', help='OAuth token endpoint URL.',
                        default=None)
    parser.add_argument('--ssl_ca_location', help='CA certificate location for SSL verification.',
                        default=None)

    return parser.parse_args()


def init_blaze():
    args = _parse_blaze_args()

    run_metadata = FireXRunMetadata(args.uid, args.logs_dir, None, None, args.firex_requester)

    blaze_dir = get_blaze_dir(run_metadata.logs_dir, args.instance_name)
    os.makedirs(blaze_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[
            logging.handlers.RotatingFileHandler(
                os.path.join(blaze_dir, 'blaze.log'),
                maxBytes=20*1024*1024, # Max size of a log file (20 MB)
                backupCount=1,
            )
        ],
        format='[%(asctime)s][%(levelname)s][%(name)s]: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S")

    logging.getLogger('kafka.producer').setLevel(logging.INFO)
    logger.info(f'Starting Blaze with args: {args}')

    signal.signal(signal.SIGTERM, lambda _, __: sys.exit(1))

    celery_app = celery_app_from_logs_dir(run_metadata.logs_dir)
    blaze_sender_config = BlazeSenderConfig(
        kafka_topic=args.kafka_topic,
        kafka_bootstrap_servers=args.bootstrap_servers.split(','),
        max_kafka_connection_retries=2,
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        sasl_oauthbearer_method=args.sasl_oauthbearer_method,
        sasl_oauthbearer_client_id=args.sasl_oauthbearer_client_id,
        sasl_oauthbearer_client_secret=args.sasl_oauthbearer_client_secret,
        sasl_oauthbearer_token_endpoint_url=args.sasl_oauthbearer_token_endpoint_url,
        ssl_ca_location=args.ssl_ca_location,
    )
    recording_file = get_blaze_events_file(run_metadata.logs_dir, args.instance_name)
    return celery_app, run_metadata, args.broker_recv_ready_file, blaze_sender_config, args.logs_url, recording_file


def main():
    celery_app, run_metadata, receiver_ready_file, blaze_sender_config, logs_url, recording_file = init_blaze()
    BlazeKafkaSenderThread(
        celery_app, run_metadata, blaze_sender_config, logs_url,
        receiver_ready_file=receiver_ready_file,
        recording_file=recording_file).run()


if __name__ == '__main__':
    main()
