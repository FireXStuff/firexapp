"""
A plugin module that lives in a sub-directory, so its tasks get dotted long names
('nested_defs.cross_plugin_defs.*') just like firex_cisco's plugins/nxpidt/ modules.

This module is NOT loaded via --plugins by the test. It is pulled in only because
cross_importing_plugin.py imports from it, which is what puts its tasks into that
plugin file's group even though it is really a separate plugin.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask

SOME_CONSTANT = 'imported-to-force-module-load'


@app.task(base=FireXTask)
def cross_helper():
    """The version that cross_plugin_defs' own tasks must keep reaching."""
    pass  # pragma: no cover


@app.task(base=FireXTask)
def task_calling_cross_helper():
    """References cross_helper from the module that defines it."""
    return cross_helper.s()
