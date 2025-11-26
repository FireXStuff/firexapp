import os

from firexapp.engine.firex_celery import FireXCelery
from celery.states import REVOKED as CELERY_REVOKED
from celery.utils.log import get_task_logger
from celery.signals import celeryd_init, task_postrun, task_revoked

from firexkit.task import get_time_from_task_start, convert_to_serializable

from firexapp.submit.install_configs import install_config_path_from_logs_dir, load_existing_install_configs
from firexapp.engine.firex_revoke import RevokeDetails
from firexapp.events.model import RunStates
from firexapp.engine.default_celery_config import FireXAppCeleryConfig

logger = get_task_logger(__name__)


app = FireXCelery()

@celeryd_init.connect
def add_items_to_conf(conf: FireXAppCeleryConfig, **_kwargs):
    conf.link_for_logo = conf.logs_dir
    conf.logs_url = None
    conf.resources_dir = app.backend.get('resources_dir').decode()

    install_config = install_config_path_from_logs_dir(conf.logs_dir)
    assert os.path.isfile(install_config), \
        f"Install config missing from run, firexapp submit is expected to have populated it: {install_config}"

    # TODO: assumes everywhere celery is started can load from logs_dir. Should likely serialize to backend.
    conf.install_config = load_existing_install_configs(conf.uid, conf.logs_dir)
    if conf.install_config.has_viewer():
        conf.logs_url = conf.link_for_logo = conf.install_config.get_logs_root_url()
        conf.link_for_logo = conf.install_config.get_logs_root_url()


@task_postrun.connect
def statsd_task_postrun(sender, task, task_id, args, kwargs, **donotcare):
    # Celery can send task-revoked event before task is completed, allowing other states (e.g. task-unblocked) to
    # be emitted after task-revoked. Sending another indicator of revoked here allows the terminal state to be
    # correctly captured by listeners, since task_postrun occurs when the task is _really_ complete.
    if task.AsyncResult(task_id).state == CELERY_REVOKED:
        try:
            RevokeDetails.write_task_revoke_complete(
                sender.app.conf.logs_dir, task_id
            )
        except Exception as e:
            logger.warning(f'Failed to write revoke complete for task {task_id}: {e}')
        task.send_event(RunStates.REVOKE_COMPLETED.to_celery_event_type())

    _send_task_completed_event(task, task_id, sender.backend)


@task_revoked.connect
def statsd_task_revoked(sender, request, terminated, signum, expired, **kwargs):
    _send_task_completed_event(sender, request.id, sender.backend)


def _send_task_completed_event(task, task_id, backend):
    actual_runtime = get_time_from_task_start(task_id, backend)
    if actual_runtime is not None:
        task.send_event(
            'task-completed',
            actual_runtime=convert_to_serializable(actual_runtime))


