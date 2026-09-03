import os

from celery import platforms
# Prevent main celery proc from killing pre-forked procs,
# otherwise killing celery main proc causes sync main firex proc
# to hang since broker will remain up.
platforms.set_pdeathsig = lambda n: None

from firexkit.task import FireXTask
from celery.utils.log import get_task_logger
from celery.signals import celeryd_init
from firexapp.submit.install_configs import install_config_path_from_logs_dir, load_existing_install_configs
from firexkit.firex_celery import FireXCelery

logger = get_task_logger(__name__)


app = FireXCelery(
    strict_typing=False,
    task_cls=f'{FireXTask.__module__}:{FireXTask.__name__}',
)
app.config_from_object('firexapp.engine.default_celery_config')


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


