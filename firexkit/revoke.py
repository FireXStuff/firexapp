import datetime
from typing import Optional

from celery.utils.log import get_task_logger
from firexkit.inspect import get_revoked

logger = get_task_logger(__name__)


class RevokedRequests:
    """
     Need to inspect the app for the revoked requests, because AsyncResult.state of a task that hasn't
    been de-queued and executed by a worker but was revoked is PENDING (i.e., the REVOKED state is only updated upon
    executing a task). This phenomenon makes the wait_for_results wait on such "revoked" tasks, and therefore
    required us to implement this work-around.
    """

    _instance = None

    @classmethod
    def instance(cls, existing_instance=None):
        if existing_instance is not None:
            cls._instance = existing_instance
        if cls._instance is None:
            cls._instance = RevokedRequests()
        return cls._instance

    def __init__(
        self,
        timer_expiry_secs: int=60,
    ):
        self.timer_expiry = datetime.timedelta(seconds=timer_expiry_secs)
        self._revoked_uuids : set[str] = set()
        self.last_updated : Optional[datetime.datetime] = _now_utc()
        from firexkit.firex_celery import FireXCelery
        self.app = FireXCelery.app_or_default()

    def _update(self) -> None:
        dests_to_revoked_uuids : dict[str, list[str]] = get_revoked(
            celery_app=self.app,
            retry_if_None_returned=False,
            timeout=60,
            destination=(f'mc@{self.app.conf.mc}', )
        ) or {}
        for dest_revoked_uuids in dests_to_revoked_uuids.values():
            self._revoked_uuids.update(dest_revoked_uuids)
        self.last_updated = _now_utc()

    def _task_in_revoked_list(self, result_id):
        return result_id in self._revoked_uuids

    def is_revoked(self, result_id: str):
        if self._task_in_revoked_list(result_id):
            return True
        # Updating the _revoked_uuids is an expensive operation, so only do it periodically
        if (
            self.last_updated is None
            or (_now_utc() - self.last_updated) > self.timer_expiry
        ):
            self._update()
            return self._task_in_revoked_list(result_id)
        else:
            return False

def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


