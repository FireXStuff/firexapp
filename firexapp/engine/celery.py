from celery.app.base import Celery
from firexkit.task import FireXTask
from celery.signals import celeryd_init


firex_app_name = '.'.join(__name__.split(".")[:-1])
app = Celery(strict_typing=False, task_cls='%s:%s' % (FireXTask.__module__, FireXTask.__name__))
app.config_from_object(firex_app_name + '.default_celery_config')


@celeryd_init.connect
def add_uid_to_conf(conf=None, **_kwargs):
    conf.uid = app.backend.get('uid').decode()
    conf.logs_dir = app.backend.get('logs_dir').decode()
    conf.link_for_logo = conf.logs_dir
    conf.resources_dir = app.backend.get('resources_dir').decode()
