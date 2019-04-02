from firexapp.engine.celery import app


# noinspection PyPep8Naming
@app.task(bind=True)
def RootTask(self, chain):
    self.enqueue_child(chain, block=True)
