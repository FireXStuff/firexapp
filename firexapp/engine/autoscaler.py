from functools import lru_cache
from time import time

from celery.worker.autoscale import Autoscaler


class FireXAutoscaler(Autoscaler):
    def __init__(self, *args, check_freq=63, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._check_freq = check_freq
        self._not_started_task_ids : set[str] = set()
        self._completed_task_ids : set[str] = set()

        from firexkit.firex_celery import FireXCelery
        self.fx_app : FireXCelery = self.worker.app

    @property
    def qty(self) -> int:
        revoked_uuids : set[str] = set(self.worker.state.revoked)
        tasks_not_started = self.update_tasks_not_started(revoked_uuids)
        started_revoked_task_ids : set[str] = revoked_uuids - tasks_not_started
        completed_task_ids = self.update_completed_task_ids(started_revoked_task_ids)
        return (
            len(self.worker.state.reserved_requests)
            + len(revoked_uuids)
            - len(completed_task_ids)
            - len(tasks_not_started)
        )

    def update_tasks_not_started(self, revoked_uuids: set[str]) -> frozenset[str]:
        self._not_started_task_ids.intersection_update(revoked_uuids)
        call_time = int(time()) // self._check_freq
        not_checked = revoked_uuids - self._not_started_task_ids
        self._not_started_task_ids.update({
            task_id
            for task_id in not_checked
            if not self._get_task_prerun_info(task_id, call_time)
        })
        return frozenset(self._not_started_task_ids)

    def update_completed_task_ids(self, started_revoked_task_ids: set[str]) -> frozenset[str]:
        call_time = int(time()) // self._check_freq
        not_done = started_revoked_task_ids - self._completed_task_ids
        self._completed_task_ids.update({
            task_id
            for task_id in not_done
            if self._get_task_postrun_info(task_id, call_time)
        })
        return frozenset(self._completed_task_ids)

    @lru_cache(maxsize=4096)
    def _get_task_postrun_info(self, task_id: str, _call_time):
        return self.fx_app.task_id_has_postrun(task_id)

    @lru_cache(maxsize=4096)
    def _get_task_prerun_info(self, task_id: str, _call_time) -> bool:
        return self.fx_app.task_id_has_prerun(task_id)
