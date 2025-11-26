import os
from typing import Optional

from firexapp.broker_manager.redis_manager import RedisManager, RedisPasswordReadError

REDIS_BIN_ENV = "redis_bin_dir"

def redis_bin_dir_from_env():
    return os.environ.get(REDIS_BIN_ENV, "")

def get_redis_bin_dir():
    redis_bin_dir = redis_bin_dir_from_env()
    if not redis_bin_dir:
        try:
            from firexapp.engine.celery import app
            redis_bin_dir = app.conf.redis_bin_dir
        except AttributeError:
            pass
    return redis_bin_dir


class BrokerFactory:
    broker_env_variable = 'BROKER'

    @classmethod
    def create_new_broker_manager(cls, logs_dir) -> RedisManager:
        return RedisManager(redis_bin_base=redis_bin_dir_from_env(), logs_dir=logs_dir)

    @classmethod
    def broker_manager_from_env(cls, logs_dir: Optional[str]) -> RedisManager:
        existing_broker_url = cls.get_broker_url(assert_if_not_set=True)
        hostname, port = RedisManager.get_hostname_port_from_url(existing_broker_url)
        return RedisManager(
            redis_bin_base=get_redis_bin_dir(),
            hostname=hostname,
            port=port,
            password=RedisManager.get_password_from_url(existing_broker_url),
            logs_dir=logs_dir,
        )

    @classmethod
    def broker_manager_from_logs_dir(cls, logs_dir, passwordless_fallback=False) -> RedisManager:
        hostname, port = RedisManager.get_hostname_port_from_logs_dir(logs_dir)
        try:
            password = RedisManager.get_password_from_logs_dir(logs_dir)
        except RedisPasswordReadError as e:
            if not passwordless_fallback:
                raise

            RedisManager.log('Cannot read previous broker password. Trying a new (random) password.', exc_info=e)
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
            raise BrokerManagerException(f'{cls.broker_env_variable} env variable has not been set')
        return url

    @classmethod
    def get_broker_failed_auth_str(cls) -> str:
        return RedisManager.get_broker_failed_auth_str()

    @staticmethod
    def get_hostname_port_from_url(broker_url):
        return RedisManager.get_hostname_port_from_url(broker_url)


class BrokerManagerException(Exception):
    pass
