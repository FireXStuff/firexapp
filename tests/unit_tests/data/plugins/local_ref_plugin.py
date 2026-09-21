"""
A plugin file that defines a task and a helper, and the task references the helper.
This allows us to assert through SignatureX that the same-plugin reference binds to
the local version even when a higher-precedence plugin also defines the helper.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def shared_helper():
    """Shared helper from local_ref_plugin."""
    pass  # pragma: no cover


@app.task(base=FireXTask)
def task_using_helper():
    """
    Task that references shared_helper within the same plugin.
    SignatureX(task_using_helper.s()).task should resolve to
    'local_ref_plugin.shared_helper', not a competing plugin's.
    """
    return shared_helper.s()
