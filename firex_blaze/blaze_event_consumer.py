"""
Process events from Celery and put them on a kafka bus.
"""

import logging
import json
import time
from getpass import getuser
from typing import Optional, Any

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient

from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.model import FireXRunMetadata, COMPLETE_RUNSTATES, RunStates

from firex_blaze.blaze_helper import BlazeSenderConfig, KAFKA_EVENTS_FILE_DELIMITER

logger = logging.getLogger(__name__)

TASK_EVENT_TO_STATE = {
    'task-started-info': 'STARTED',
    RunStates.FAILED.to_celery_event_type(): 'FAILURE',
    RunStates.SUCCEEDED.to_celery_event_type(): 'SUCCESS',
    RunStates.REVOKED.to_celery_event_type(): 'REVOKED',
    RunStates.REVOKE_COMPLETED.to_celery_event_type(): 'REVOKED',
    # historically were mapped but never sent.
    # 'task-sent': 'PENDING',
    # 'task-received': 'RECEIVED',
    # 'task-started': 'STARTED',
    # 'task-rejected': 'REJECTED',
    # 'task-retried': 'RETRY',
}

BLAZE_SEND_EVENT_TYPES = tuple(
    list(TASK_EVENT_TO_STATE.keys()) + ['task-completed', 'task-results', 'task-instrumentation', 'task-args']
)


def format_kafka_message(firex_id, event_data, uuid, logs_url, submitter=getuser(), firex_requester=None) -> dict[str, Any]:
    return {'FIREX_ID': firex_id,
            'SUBMITTER': submitter,
            'FIREX_REQUESTER': firex_requester,
            'LOGS_URL': logs_url,               # Shouldn't be required, but Lumens needs it!
            'EVENTS': [{'DATA': event_data,
                        'UUID': uuid}]}



def send_kafka_mssg(kafka_producer: Producer, kafka_mssg: dict[str, Any], kafka_topic: str, firex_id: str,
                    partition: Optional[int] = None):
    """Send message using confluent-kafka Producer."""
    try:
        kafka_producer.produce(
            topic=kafka_topic,
            value=json.dumps(kafka_mssg).encode('ascii'),
            key=firex_id.encode('ascii'),
        )
        # Poll to handle any internal events and queued delivery reports (in case of delivery callback functions)
        kafka_producer.poll(0)
    except BufferError:
        # Buffer is full, wait for messages to be delivered
        logger.warning('Kafka producer buffer full, waiting for delivery...')
        kafka_producer.poll(1)
        # Retry the send
        kafka_producer.produce(
            topic=kafka_topic,
            value=json.dumps(kafka_mssg).encode('ascii'),
            key=firex_id.encode('ascii'),
        )
        kafka_producer.poll(0)
    except KafkaException as e:
        logger.error(f'Failed to send Kafka message: {e}')


def get_basic_event(name, event_type, timestamp=None, event_timestamp=time.time()):
    if timestamp is None:
        timestamp = event_timestamp

    event_data = {'name': name,
                  'type': event_type,
                  'timestamp': timestamp,               # Shouldn't be required, but Lumens needs it!
                  'event_timestamp': event_timestamp}

    # Not all types map to states (e.g. task-results), so only populate state for some event types.
    if event_type in TASK_EVENT_TO_STATE:
        event_data['state'] = TASK_EVENT_TO_STATE[event_type]

    return event_data


class NoNameForEvent(Exception):
    pass


class KafkaSenderThread(BrokerEventConsumerThread):

    def __init__(
        self,
        celery_app,
        run_metadata: FireXRunMetadata,
        config: BlazeSenderConfig,
        max_retry_attempts: Optional[int] = None,
        receiver_ready_file: Optional[str] = None,
        recording_file: Optional[str] = None,
        partition: Optional[int] = None,
    ):

        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)
        self.firex_id = run_metadata.firex_id
        self.kafka_topic = config.kafka_topic
        self.recording_file = recording_file
        self.partition = partition

        # Connect to bootstrap servers and get a Producer instance
        self.producer = self.get_kafka_producer(config)
        logger.debug (f'Kafka producer created for topic {self.kafka_topic}')
        self.root_task = {'uuid': None, 'is_complete': False}

    @classmethod
    def get_kafka_producer(cls, config: BlazeSenderConfig) -> Producer:
        """Create confluent-kafka Producer with retry logic."""
        _retries = 0

        # Build configuration dictionary for confluent-kafka
        producer_config = {
            'bootstrap.servers': ','.join(config.kafka_bootstrap_servers)
                if isinstance(config.kafka_bootstrap_servers, list)
                else config.kafka_bootstrap_servers,
            'security.protocol': config.security_protocol,
        }

        # Add SASL-SSL OAuth parameters
        if config.sasl_mechanism:
            producer_config['sasl.mechanism'] = config.sasl_mechanism
        if config.sasl_oauthbearer_method:
            producer_config['sasl.oauthbearer.method'] = config.sasl_oauthbearer_method
        if config.sasl_oauthbearer_client_id:
            producer_config['sasl.oauthbearer.client.id'] = config.sasl_oauthbearer_client_id
        if config.sasl_oauthbearer_client_secret:
            producer_config['sasl.oauthbearer.client.secret'] = config.sasl_oauthbearer_client_secret
        if config.sasl_oauthbearer_token_endpoint_url:
            producer_config['sasl.oauthbearer.token.endpoint.url'] = config.sasl_oauthbearer_token_endpoint_url
        if config.ssl_ca_location:
            producer_config['ssl.ca.location'] = config.ssl_ca_location

        while True:
            try:
                # Create the producer - it will handle connection and authentication
                producer = Producer(producer_config)
                logger.info('Successfully created Kafka producer')
                return producer

            except KafkaException as e:
                if _retries < config.max_kafka_connection_retries:
                    _retries += 1
                    logger.exception(e)
                    logger.warning(f'Retrying connecting to bootstrap servers '
                                   f'[retry {_retries}/{config.max_kafka_connection_retries}]')
                    time.sleep(min(2 ** _retries, 30))  # Exponential backoff
                else:
                    logger.error(f'Failed to connect to Kafka after {config.max_kafka_connection_retries} retries')
                    raise

    def _is_root_complete(self):
        return self.root_task['is_complete']

    def _update_root_task(self, event):
        if (
            event.get('type') == 'task-received'
            and 'root_id' in event
            and self.root_task['uuid'] is None
        ):
            self.root_task['uuid'] = event['root_id']

        if (
            event['uuid'] == self.root_task['uuid']
            # crazy things can happen with the celery task state model;
            # avoid switching out of completed.
            and RunStates.is_complete_state(event.get('type'))
        ):
            self.root_task['is_complete'] = True

    def _send_celery_event_to_kafka(self, celery_event: dict[str, Any]) -> list[dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement sending.")

    def _on_celery_event(self, event):
        if 'uuid' not in event:
            return

        self._update_root_task(event)

        sent_kafka_events = self._send_celery_event_to_kafka(event)

        if sent_kafka_events and self.recording_file:
            # Append the event to the recording file.
            with open(self.recording_file, "a") as rec:
                for e in sent_kafka_events:
                    event_data_str = json.dumps(e, sort_keys=True, indent=2)
                    rec.write(event_data_str + KAFKA_EVENTS_FILE_DELIMITER)

    def _on_cleanup(self):
        """Flush and close the producer with proper timeout handling."""
        logger.info('Flushing Kafka producer...')

        # Flush with timeout - returns number of messages still in queue
        remaining = self.producer.flush(timeout=120)

        if remaining > 0:
            logger.warning(f'{remaining} messages were not delivered before timeout')
        else:
            logger.info('All messages successfully delivered to Kafka')


class BlazeKafkaSenderThread(KafkaSenderThread):
    """Captures Celery events and puts them on a Kafka bus."""

    def __init__(self,
                 celery_app,
                 run_metadata: FireXRunMetadata,
                 config: BlazeSenderConfig,
                 logs_url: str,
                 max_retry_attempts: Optional[int] = None,
                 receiver_ready_file: Optional[str] = None,
                 recording_file: Optional[str] = None,
    ):

        super().__init__(
            celery_app, run_metadata, config, max_retry_attempts,
            receiver_ready_file, recording_file)

        self.submitter = getuser()
        self.firex_requester = run_metadata.firex_requester
        self.firex_id = run_metadata.firex_id
        self.logs_url = logs_url
        self.kafka_topic = config.kafka_topic
        self.uuid_to_task_name_mapping : dict[str, str] = {}

    def _get_kafka_event(self, event: dict[str, Any]) -> dict[str, Any]:
        uuid = event.pop('uuid')
        if uuid not in self.uuid_to_task_name_mapping and 'long_name' in event:
            self.uuid_to_task_name_mapping[uuid] = event['long_name']

        if uuid in self.uuid_to_task_name_mapping:
            task_name = self.uuid_to_task_name_mapping[uuid]
        else:
            # No need to produce this event since it won't be processed by Lumens anyways
            raise NoNameForEvent(f'No task name found for {event}; can not send the event')

        # Remove result since we only should report firex_result, not the native result
        event.pop('result', None)

        basic_event_data = get_basic_event(
            name=task_name,
            event_type=event.get('type'),
            timestamp=event['timestamp'],
            # Add the event_timestamp (copy of the local_received), since the native timestamp that
            # Celery provides is broken (its local time instead of UTC, and utcoffset is inaccurate).
            # This piece of -redundant- data is just because Lumens can't make local_received query-able
            event_timestamp=event['local_received'])

        return format_kafka_message(
            firex_id=self.firex_id,
            event_data=event | basic_event_data,
            uuid=uuid,
            logs_url=self.logs_url,
            submitter=self.submitter,
            firex_requester=self.firex_requester)

    def _send_celery_event_to_kafka(self, celery_event: dict[str, Any]) -> list[dict[str, Any]]:
        if celery_event.get('type') in BLAZE_SEND_EVENT_TYPES:
            try:
                kafka_event = self._get_kafka_event(celery_event)
            except NoNameForEvent as e:
                logger.exception(e)
            else:
                send_kafka_mssg(kafka_producer=self.producer,
                                kafka_mssg=kafka_event,
                                kafka_topic=self.kafka_topic,
                                firex_id=self.firex_id,
                                partition=self.partition)
                return [kafka_event]

        return []
