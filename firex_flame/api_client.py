"""
Client-side access to a running Flame server's HTTP API.

Kept separate from :mod:`firex_flame.api`, which serves that API and therefore
imports flask, gevent and socketio at module scope. Celery workers and submit
processes only ever need the client half, and they import it from the service
discovery manifest, so pulling the server stack in with it leaves ~18 MB of web
server (446 module objects) resident in every FireX process for its whole life.
"""

import getpass
import logging
import os
import urllib.parse

import requests

from firex_flame.flame_helper import REVOKE_REASON_KEY
from firex_flame.model_dumper import wait_and_get_flame_url

logger = logging.getLogger(__name__)


def flame_revoke(
    logs_dir: str,
    task_uuid: str
    | None = None,  # None revokes the whole run by revoking the root task.
    revoke_reason: str | None = None,
    revoking_user: str | None = getpass.getuser(),
    timeout=10 * 60,
) -> requests.Response | None:

    flame_url = wait_and_get_flame_url(firex_logs_dir=logs_dir)
    if not flame_url:
        logger.warning(
            f"Flame URL not found for {logs_dir}; revoke via Flame will likely fail."
        )
    else:
        # requesting /api/revoke will revoke the root task, which revoked the entire run.
        url_path = "/api/revoke"
        if task_uuid:
            url_path = os.path.join(url_path, task_uuid)

        url_params = {"revoking_user": revoking_user}
        if revoke_reason:
            url_params[REVOKE_REASON_KEY] = revoke_reason

        return requests.get(
            urllib.parse.urljoin(flame_url, url_path),
            params=url_params,
            timeout=timeout,
        )

    return None
