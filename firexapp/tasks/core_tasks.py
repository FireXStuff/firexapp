from firexapp.application import get_app_tasks
from firexapp.engine.celery import app
from firexkit.chain import InjectArgs


# noinspection PyPep8Naming
@app.task(bind=True)
def RootTask(self, chain, **chain_args):
    c = InjectArgs(chain=chain, **chain_args)
    for task in get_app_tasks(chain):
        c |= task.s()
    self.enqueue_child(c, block=True)
