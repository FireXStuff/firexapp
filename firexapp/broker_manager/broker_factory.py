import os
import logging
from firexapp.broker_manager import BrokerManager


class BrokerFactory:
    broker_env_variable = 'BROKER'
    _broker = None

    @classmethod
    def set_broker_manager(cls, manager):
        if not isinstance(manager, BrokerManager):
            raise BrokerManagerException("Object %s is not of type BrokerManager" % str(manager))
        cls.set_broker_env(manager.get_url())
        manager.log('export %s=%s' % (cls.broker_env_variable, manager.get_url()), level=logging.INFO)
        cls._broker = manager

    @classmethod
    def set_broker_env(cls, broker_url):
        os.environ[cls.broker_env_variable] = broker_url

    @classmethod
    def get_broker_manager(cls, *args, **kwargs)->BrokerManager:
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
        broker = RedisManager(*args, redis_bin_base=redis_bin_dir, **kwargs)
        app.conf.result_backend = broker.get_url()
        cls.set_broker_manager(broker)
        return broker

    @classmethod
    def get_broker_url(cls, assert_if_not_set=False)->str:
        url = os.environ.get(cls.broker_env_variable, "")
        if assert_if_not_set and not url:
            raise '%s env variable has not been set' % cls.broker_env_variable
        return url

class BrokerManagerException(Exception):
    pass
