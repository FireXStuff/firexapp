from celery import platforms

# Prevent main celery proc from killing pre-forked procs,
# otherwise killing celery main proc causes sync main firex proc
# to hang since broker will remain up.
platforms.set_pdeathsig = lambda n: None

from celery.utils.log import get_task_logger

from firexapp.celery_manager import CeleryManager
from firexapp.engine.default_celery_config import FxEnvVars
from firexkit.firex_celery import FireXCelery

logger = get_task_logger(__name__)

if CeleryManager.is_current_env_fx_celery_worker():
    # this is in a Celery main process with infra already
    # started, including metadata in backend. Attach
    # to existing Redis+Celery and load data from backend.
    app = FireXCelery.set_worker_fx_app()
else:
    app = FireXCelery(
        fx_env=FxEnvVars.create_no_task_exec_fx_env(),
        fx_expect_tasks=False,
    )
