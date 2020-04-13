import logging
from _socket import gethostname


def add_hostname_to_log_records():
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.hostname = gethostname()
        return record

    logging.setLogRecordFactory(record_factory)