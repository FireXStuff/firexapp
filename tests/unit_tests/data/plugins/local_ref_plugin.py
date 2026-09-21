"""
A plugin file that defines a task and a helper, and the task references the helper.
This allows us to assert through SignatureX which version that same-plugin reference
binds to once a separate, higher-precedence plugin file also defines the helper.
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
    Task that references shared_helper within the same plugin. A plugin file listed
    after this one must still be able to intercept that reference, so
    SignatureX(task_using_helper.s()).task resolves to the overriding plugin's
    shared_helper rather than this file's.
    """
    return shared_helper.s()
