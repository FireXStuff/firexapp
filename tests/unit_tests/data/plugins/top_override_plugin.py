"""
The higher-precedence half of a three-link override chain, so that
mid_override_plugin's override is itself overridden. This is the production shape:
sparse_build.JamBuild overrides ctc_plugin.JamBuild, which overrides the core
microservices.tasks.JamBuild.
"""

from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def chained_override_me():
    """The dominant override, which calls its orig -- mid_override_plugin's."""
    # pragma: no cover
