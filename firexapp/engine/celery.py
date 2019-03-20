from celery.app.base import Celery
from firexkit.task import FireXTask

firex_app_name = '.'.join(__name__.split(".")[:-1])
app = Celery(strict_typing=False, task_cls='%s:%s' % (FireXTask.__module__, FireXTask.__name__))
app.config_from_object(firex_app_name + '.default_celery_config')
