from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def override_me():
    pass  # pragma: no cover
