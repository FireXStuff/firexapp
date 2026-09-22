"""
A separate, higher-precedence plugin file that also defines shared_helper, to test
that it wins everywhere -- including for the reference from within local_ref_plugin,
whose whole point is to be interceptable by a plugin listed after it.
"""

from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def shared_helper():
    """Shared helper from competing_helper_plugin (higher precedence)."""
    # pragma: no cover
