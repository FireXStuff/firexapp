from typing import Optional
import logging

from celery.app.base import Celery
from firexapp.reporters.json_reporter import RevokeDetails, FireXRunData

logger = logging.getLogger(__name__)


class FireXRunController:

    def __init__(
        self,
        celery_app: Optional[Celery]=None,
        logs_dir: Optional[str]=None,
    ):
        #
        # Celery apps are a nightmare, and sometimes we inspect from contexts where
        # firex hasn't initialised the app, so sometimes it's illegal to inspect
        # app.conf.logs_dir. Ideally everything would be initialised earlier, even
        # if that means tracking the app's logs_dir seperately from app.conf init.
        #
        # This means it's unfortunately always possible some op might fair from
        # FireXRunController.
        #
        if logs_dir:
            self.logs_dir = logs_dir
        elif celery_app:
            self.logs_dir = celery_app.conf.logs_dir
        else:
            raise AssertionError('Must supply at least one of celery_app, logs_dir')
        self.celery_app = celery_app
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
            (self.celery_app and _backend_get_root_revoked(self.celery_app))
            or self.run_revoke_complete()
        )

    def get_current_run_revoke(self) -> Optional[RevokeDetails]:
        if self.celery_app and _backend_get_root_revoked(self.celery_app):
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
def _backend_set_root_revoked(celery_app: Celery):
    celery_app.backend.set(_RUN_REVOKE_STARTED_KEY, 'True')

#
# Get flag in backend DB (i.e.: Redis) which indicates root taks has been revoked
#
def _backend_get_root_revoked(celery_app: Celery):
    return celery_app.backend.get(_RUN_REVOKE_STARTED_KEY)