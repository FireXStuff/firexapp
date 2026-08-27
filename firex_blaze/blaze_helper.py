"""
    Utility functions for the firex_blaze package.
"""
import os
from dataclasses import dataclass
import json
from typing import Optional

from celery.app.base import Celery

from firexapp.events.event_aggregator import FireXEventAggregator
from firexapp.broker_manager.broker_factory import RedisManager
from firex_blaze.fast_blaze_helper import get_blaze_dir

KAFKA_EVENTS_FILE_DELIMITER = '--END_OF_EVENT--'


@dataclass
class BlazeSenderConfig:
    kafka_topic: str
    kafka_bootstrap_servers: list[str]
    max_kafka_connection_retries: int
    security_protocol: str = 'PLAINTEXT'
    # SASL-SSL OAuth 2.0 parameters
    sasl_mechanism: Optional[str] = None
    sasl_oauthbearer_method: Optional[str] = None
    sasl_oauthbearer_client_id: Optional[str] = None
    sasl_oauthbearer_client_secret: Optional[str] = None
    sasl_oauthbearer_token_endpoint_url: Optional[str] = None
    ssl_ca_location: Optional[str] = None


def get_blaze_events_file(logs_dir, instance_name=None):
    return os.path.join(get_blaze_dir(logs_dir, instance_name), 'kafka_events.json')


def get_kafka_events(logs_dir, instance_name=None):
    import gzip
    real_rec = os.path.realpath(get_blaze_events_file(logs_dir, instance_name))
    if real_rec.endswith('.gz'):
        with gzip.open(real_rec, 'rt', encoding='utf-8') as rec:
            all_text = rec.read()
    else:
        with open(real_rec) as rec:
            all_text = rec.read()
    event_records = all_text.split(sep=KAFKA_EVENTS_FILE_DELIMITER)
    return [
        json.loads(e) for e in event_records
        if e
    ]


def aggregate_blaze_kafka_msgs(firex_id, kafka_msgs):
    event_aggregator = FireXEventAggregator()
    for kafka_event in kafka_msgs:
        if kafka_event['FIREX_ID'] == firex_id:
            inner_event = kafka_event['EVENTS'][0]
            celery_event = dict(inner_event['DATA'])
            celery_event['uuid'] = inner_event['UUID']
            event_aggregator.aggregate_events([celery_event])

    return event_aggregator.tasks_by_uuid


def celery_app_from_logs_dir(logs_dir):
    return Celery(broker=RedisManager.get_broker_url_from_logs_dir(logs_dir),
                  accept_content=['pickle', 'json'])