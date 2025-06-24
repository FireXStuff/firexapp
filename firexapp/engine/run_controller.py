from typing import Optional
import logging

from celery.app.base import Celery

from firexapp.engine.celery import app
from firexapp.reporters.json_reporter import RevokeDetails, FireXRunData

logger = logging.getLogger(__name__)


class FireXRunController:

    def __init__(
        self,
        celery_app: Celery=app,
        logs_dir: Optional[str]=None,
    ):
        assert celery_app or logs_dir, 'Must supply celery_app or logs_dir'
        self.celery_app = celery_app
        self.logs_dir = logs_dir or self.celery_app.conf.logs_dir
        self._run_revoke_dir: Optional[str] = None

    def revoke_task(
        self,
        task_uuid: str,
        revoke_reason: str,
        revoking_user: str,
        is_root_task: bool=False,
    ):
        assert self.celery_app, 'Can only revoke when initialised with celery app.'

        # revoke can be fast, so do data tracking setup before control.revoke()
        if is_root_task:
            _backend_set_root_revoked(self.celery_app)

        RevokeDetails(
            self.logs_dir,
            reason=revoke_reason,
            task_uuid=task_uuid,
            root_revoke=is_root_task,
            revoking_user=revoking_user,
        ).write()
        self.celery_app.control.revoke(task_uuid, terminate=True)
        logger.info(f"Submitted revoke to celery for: {task_uuid}")


    def is_run_revoke_started(self) -> bool:
        return (
            _backend_get_root_revoked(self.celery_app)
            or self.run_revoke_complete()
        )

    def get_current_run_revoke(self) -> Optional[RevokeDetails]:
        if not _backend_get_root_revoked(self.celery_app):
            return None
        return RevokeDetails.load_latest_run_revoke_details(
            self.logs_dir,
        )

    def run_revoke_complete(self) -> bool:
        return FireXRunData.load_from_logs_dir(
            self.logs_dir,
        ).revoked

_RUN_REVOKE_STARTED_KEY = 'ROOT_REVOKED'

#
# Set flag in backend DB (i.e.: Redis) to indicate root taks has been revoked
#
def _backend_set_root_revoked(celery_app):
    celery_app.backend.set(_RUN_REVOKE_STARTED_KEY, 'True')

#
# Get flag in backend DB (i.e.: Redis) which indicates root taks has been revoked
#
def _backend_get_root_revoked(celery_app):
    return celery_app.backend.get(_RUN_REVOKE_STARTED_KEY)