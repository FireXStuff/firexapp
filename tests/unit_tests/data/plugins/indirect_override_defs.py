from firexapp.engine.celery import app
from firexkit.task import FireXTask


# This microservice is NOT defined in a plugin file itself: it's defined in a module
# that a plugin file imports, which must still override the original.
@app.task(base=FireXTask)
def indirect_override_me():
    pass  # pragma: no cover
