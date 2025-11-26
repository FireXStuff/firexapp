from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task()
def override_me():
    pass  # pragma: no cover