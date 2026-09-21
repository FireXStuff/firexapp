"""
A higher-precedence plugin that also defines shared_helper, to test that
references to it from outside the plugins get this version, but references
from within local_ref_plugin still get that plugin's version.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def shared_helper():
    """Shared helper from competing_helper_plugin (higher precedence)."""
    pass  # pragma: no cover
