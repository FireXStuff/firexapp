from abc import ABC, abstractmethod
import logging
from firexapp.submit.console import setup_console_logging

logger = setup_console_logging(__name__)


class BrokerManager(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def get_url(self) -> str:
        pass

    @abstractmethod
    def is_alive(self) -> bool:
        pass

    @classmethod
    def log(cls, msg, header=None, level=logging.DEBUG, exc_info=None):
        if header is None:
            header = cls.__name__
        if header:
            msg = '[%s] %s' % (header, msg)
        logger.log(level, msg, exc_info=exc_info)
