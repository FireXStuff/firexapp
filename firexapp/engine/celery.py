import os

from celery import platforms
# Prevent main celery proc from killing pre-forked procs,
# otherwise killing celery main proc causes sync main firex proc
# to hang since broker will remain up.
platforms.set_pdeathsig = lambda n: None

from celery.app.base import Celery
from firexkit.task import FireXTask, get_time_from_task_start, convert_to_serializable
from celery.states import REVOKED as CELERY_REVOKED
from celery.utils.log import get_task_logger
from celery.signals import celeryd_init, task_postrun, task_revoked
from firexapp.submit.install_configs import install_config_path_from_logs_dir, load_existing_install_configs
from firexapp.engine.firex_revoke import RevokeDetails
from firexapp.events.model import RunStates



logger = get_task_logger(__name__)

firex_app_name = '.'.join(__name__.split(".")[:-1])
app = Celery(strict_typing=False, task_cls='%s:%s' % (FireXTask.__module__, FireXTask.__name__))
app.config_from_object(firex_app_name + '.default_celery_config')


@celeryd_init.connect
def add_items_to_conf(conf=None, **_kwargs):
    conf.uid = app.backend.get('uid').decode()
    conf.logs_dir = app.backend.get('logs_dir').decode()
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
