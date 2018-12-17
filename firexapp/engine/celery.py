from celery.app.base import Celery

firex_app_name = '.'.join(__name__.split(".")[:-1])
app = Celery(strict_typing=False)
app.config_from_object(firex_app_name + '.default_celery_config')
