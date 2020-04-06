import os
from firexapp.broker_manager import BrokerManager
from firexapp.broker_manager.redis_manager import RedisManager
from firexapp.engine.celery import app

REDIS_BIN_ENV = "redis_bin_dir"

def get_redis_bin_dir():
    redis_bin_dir = os.environ.get(REDIS_BIN_ENV, "")
    if not redis_bin_dir:
        try:
            redis_bin_dir = app.conf.redis_bin_dir
        except AttributeError:
            pass
    return redis_bin_dir


class BrokerFactory:
    broker_env_variable = 'BROKER'

    @classmethod
    def set_broker_env(cls, broker_url):
        os.environ[cls.broker_env_variable] = broker_url

    @classmethod
    def create_new_broker_manager(cls, *args, **kwargs) -> BrokerManager:
        return RedisManager(*args, redis_bin_base=get_redis_bin_dir(), **kwargs)

    @classmethod
    def broker_manager_from_env(cls) -> BrokerManager:
        existing_broker_url = cls.get_broker_url(assert_if_not_set=True)
        hostname, port = RedisManager.get_hostname_port_from_url(existing_broker_url)
        return RedisManager(hostname=hostname,
                            port=port,
                            redis_bin_base=get_redis_bin_dir())

    @classmethod
    def get_broker_url_from_logs_dir(cls, logs_dir) -> str:
        return RedisManager.get_broker_url_from_metadata(logs_dir)

    @classmethod
    def broker_manager_from_logs_dir(cls, logs_dir) -> BrokerManager:
        existing_broker_url = cls.get_broker_url_from_logs_dir(logs_dir)
        hostname, port = RedisManager.get_hostname_port_from_url(existing_broker_url)
        return RedisManager(hostname=hostname,
                            port=port,
                            # FIXME: The RedisManager class offers different capabilities depending on how it's
                            #  initialized. Some functions don't work if it doesn't have a logs_dir.
                            logs_dir=logs_dir,
                            redis_bin_base=get_redis_bin_dir())

    @classmethod
    def get_broker_url(cls, assert_if_not_set=False)-> str:
        url = os.environ.get(cls.broker_env_variable, "")
        if assert_if_not_set and not url:
            raise BrokerManagerException('%s env variable has not been set' % cls.broker_env_variable)
        return url


class BrokerManagerException(Exception):
    pass
