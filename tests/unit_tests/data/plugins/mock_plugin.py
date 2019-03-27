from firexapp.engine.celery import app
from firexkit.task import FireXTask
from celery.signals import task_postrun


@app.task(base=FireXTask)
def override_me():
    pass  # pragma: no cover


# add a signal to the microservice that will be overridden to make sure signals are handled
# noinspection PyUnusedLocal
@task_postrun.connect(sender=override_me)
def override_me_ran(sender, request, terminated, signum, expired, **kwargs):
    pass
