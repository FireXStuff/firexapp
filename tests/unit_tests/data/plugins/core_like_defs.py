"""
Stands in for a core task module that a plugin file happens to be the first thing to
import -- firex_cisco's testsuites.firex_build, for instance. The test adds this
module to app.conf.imports, which is what makes the registry treat it as core.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def core_and_plugin_task():
    """Core's version, which a plugin is entitled to override."""
    pass  # pragma: no cover
