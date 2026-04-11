"""Helpers for storing and reading run-level revocation state."""

from typing import Any, Optional

from celery.app.base import Celery
from celery.states import RETRY, REVOKED

ROOT_REVOKED_BACKEND_KEY = 'ROOT_REVOKED'


def backend_set_root_revoked(celery_app: Celery) -> None:
    """Persist that the run has been revoked."""
    celery_app.backend.set(ROOT_REVOKED_BACKEND_KEY, 'True')


def backend_get_root_revoked(celery_app: Celery) -> Optional[Any]:
    """Return the persisted run revocation marker."""
    return celery_app.backend.get(ROOT_REVOKED_BACKEND_KEY)


def is_root_revoked(celery_app: Celery, result_state: Optional[str] = None) -> bool:
    """Return whether the run should be treated as revoked."""
    if result_state in [REVOKED, RETRY]:
        return True

    revoked = backend_get_root_revoked(celery_app)
    return revoked is not None and revoked.decode().lower() == 'true'
