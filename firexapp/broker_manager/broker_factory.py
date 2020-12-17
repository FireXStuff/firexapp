import os

from firexapp.broker_manager.redis_manager import RedisManager, RedisPasswordReadError
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
    def create_new_broker_manager(cls, *args, **kwargs) -> RedisManager:
        return RedisManager(*args, redis_bin_base=get_redis_bin_dir(), **kwargs)

    @classmethod
    def broker_manager_from_env(cls) -> RedisManager:
        existing_broker_url = cls.get_broker_url(assert_if_not_set=True)
        hostname, port = RedisManager.get_hostname_port_from_url(existing_broker_url)
        password = RedisManager.get_password_from_url(existing_broker_url)
        return RedisManager(redis_bin_base=get_redis_bin_dir(),
                            hostname=hostname,
                            port=port,
                            password=password)

    @classmethod
    def broker_manager_from_logs_dir(cls, logs_dir, passwordless_fallback=False) -> RedisManager:
        hostname, port = RedisManager.get_hostname_port_from_logs_dir(logs_dir)
        try:
            password = RedisManager.get_password_from_logs_dir(logs_dir)
        except RedisPasswordReadError as e:
            if not passwordless_fallback:
                raise

            RedisManager.log(f'Cannot read previous broker password. Trying a new (random) password.', exc_info=e)
            # Setting this to None will cause the broker manager to create a new password
            password = None

        return RedisManager(redis_bin_base=get_redis_bin_dir(),
                            hostname=hostname,
                            port=port,
                            # FIXME: The RedisManager class offers different capabilities depending on how it's
                            #  initialized. Some functions don't work if it doesn't have a logs_dir.
                            logs_dir=logs_dir,
                            password=password)

    @classmethod
    def get_broker_url_from_logs_dir(cls, logs_dir) -> str:
        return RedisManager.get_broker_url_from_logs_dir(logs_dir)

    @classmethod
    def get_broker_url(cls, assert_if_not_set=False) -> str:
        url = os.environ.get(cls.broker_env_variable, "")
        if assert_if_not_set and not url:
            raise BrokerManagerException('%s env variable has not been set' % cls.broker_env_variable)
        return url

    @classmethod
    def get_broker_failed_auth_str(cls) -> str:
        return RedisManager.get_broker_failed_auth_str()

    @staticmethod
    def get_hostname_port_from_url(broker_url):
        return RedisManager.get_hostname_port_from_url(broker_url)


class BrokerManagerException(Exception):
    pass
