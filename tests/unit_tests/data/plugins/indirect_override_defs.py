from firexapp.engine.celery import app
from firexkit.task import FireXTask


# This microservice is NOT defined in a plugin file itself: it's defined in a module
# that a plugin file imports. Merely being imported earns it no plugin precedence, so
# it must not displace the override of a plugin that *was* listed.
@app.task(base=FireXTask)
def indirect_override_me():
    pass  # pragma: no cover
