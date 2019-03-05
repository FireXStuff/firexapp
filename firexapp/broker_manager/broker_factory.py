import os
import logging
from firexapp.broker_manager import BrokerManager


class BrokerFactory:
    _broker = None

    @classmethod
    def set_broker_manager(cls, manager):
        if not isinstance(manager, BrokerManager):
            raise BrokerManagerException("Object %s is not of type BrokerManager" % str(manager))
        os.environ['BROKER'] = manager.get_url()
        manager.log('export BROKER=%s' % manager.get_url(), level=logging.INFO)
        cls._broker = manager

    @classmethod
    def get_broker_manager(cls)->BrokerManager:
        if cls._broker:
            return cls._broker

        from firexapp.engine.celery import app

        redis_bin = os.environ.get("redis_bin")
        if not redis_bin:
            try:
                redis_bin = app.conf.redis_bin
            except AttributeError:
                pass

        if redis_bin:
            if not os.path.isdir(redis_bin):
                raise FileNotFoundError("'redis_bin' in environment is not a valid directory")
            from firexapp.broker_manager.redis_manager import RedisManager
            broker = RedisManager(redis_bin_base=redis_bin)
            app.conf.result_backend = broker.get_url()
            cls.set_broker_manager(broker)
            return broker

    @classmethod
    def get_broker_url(cls)->str:
        return os.environ.get('BROKER', "")


class BrokerManagerException(Exception):
    pass
