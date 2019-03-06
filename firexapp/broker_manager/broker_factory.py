import os
import logging
from firexapp.broker_manager import BrokerManager


class BrokerFactory:
    BROKER_ENV_VARIABLE = 'BROKER'
    _broker = None

    @classmethod
    def set_broker_manager(cls, manager):
        if not isinstance(manager, BrokerManager):
            raise BrokerManagerException("Object %s is not of type BrokerManager" % str(manager))
        os.environ[cls.BROKER_ENV_VARIABLE] = manager.get_url()
        manager.log('export %s=%s' % (cls.BROKER_ENV_VARIABLE, manager.get_url()), level=logging.INFO)
        cls._broker = manager

    @classmethod
    def get_broker_manager(cls)->BrokerManager:
        if cls._broker:
            return cls._broker

        from firexapp.engine.celery import app

        redis_bin_dir = os.environ.get("redis_bin_dir", "")
        if not redis_bin_dir:
            try:
                redis_bin_dir = app.conf.redis_bin_dir
            except AttributeError:
                pass

        from firexapp.broker_manager.redis_manager import RedisManager
        broker = RedisManager(redis_bin_base=redis_bin_dir)
        app.conf.result_backend = broker.get_url()
        cls.set_broker_manager(broker)
        return broker

    @classmethod
    def get_broker_url(cls)->str:
        return os.environ.get(cls.BROKER_ENV_VARIABLE, "")


class BrokerManagerException(Exception):
    pass
