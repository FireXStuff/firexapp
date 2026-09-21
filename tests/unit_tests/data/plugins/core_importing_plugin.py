"""
A plugin file that is the first thing to import a core task module, and that also
defines its own version of a task that core module defines.

Being imported by a plugin must lend the core module neither the plugin's precedence
nor plugin ownership of its tasks: either one would leave core's version in place
instead of this plugin's override.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask

import core_like_defs  # noqa: F401


@app.task(base=FireXTask)
def core_and_plugin_task():
    """The plugin's override, which must win everywhere."""
    pass  # pragma: no cover
