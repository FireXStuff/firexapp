import os

from firexapp.broker_manager.redis_manager import RedisManager, RedisPasswordReadError


class BrokerFactory:

    @classmethod
    def get_redis_bin_dir(cls) -> str:
        return os.environ.get("redis_bin_dir", "")

    @classmethod
    def load_broker_manager(
        cls,
        logs_dir: str,
        redis_bin_base: str | None=None,
        broker_url: str | None=None,
    ) -> RedisManager:
        if broker_url:
            hostname, port = RedisManager.get_hostname_port_from_url(broker_url)
            password=RedisManager.get_password_from_url(broker_url)
        else:
            hostname = port = password = None

        return RedisManager(
            redis_bin_base=redis_bin_base or cls.get_redis_bin_dir(),
            hostname=hostname,
            port=port,
            password=password,
            logs_dir=logs_dir,
        )

    @classmethod
    def broker_manager_from_logs_dir(
        cls,
        logs_dir: str,
        passwordless_fallback=False,
    ) -> RedisManager:
        hostname, port = RedisManager.get_hostname_port_from_logs_dir(logs_dir)
        try:
            password = RedisManager.get_password_from_logs_dir(logs_dir)
        except RedisPasswordReadError as e:
            if not passwordless_fallback:
                raise
            RedisManager.log('Cannot read previous broker password. Trying a new (random) password.', exc_info=e)
            # Setting this to None will cause the broker manager to create a new password
            password = None

        return RedisManager(
            redis_bin_base=cls.get_redis_bin_dir(),
            hostname=hostname,
            port=port,
            logs_dir=logs_dir,
            password=password,
        )

    @classmethod
    def get_broker_url_from_logs_dir(cls, logs_dir) -> str:
        return RedisManager.get_broker_url_from_logs_dir(logs_dir)

    @classmethod
    def get_broker_failed_auth_str(cls) -> str:
        return RedisManager.get_broker_failed_auth_str()


class BrokerManagerException(Exception):
    pass
