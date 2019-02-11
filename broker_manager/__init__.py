from abc import ABC, abstractmethod
import logging
from firexapp.submit.submit import setup_console_logging

logger = setup_console_logging(__name__)


class BrokerManager(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @classmethod
    def log(cls, msg, header=None, level=logging.DEBUG):
        if header is None:
            header = cls.__name__
        if header:
            msg = '[%s] %s' % (header, msg)
        logger.log(level, msg)