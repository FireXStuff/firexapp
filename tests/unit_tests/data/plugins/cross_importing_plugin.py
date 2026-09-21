"""
A plugin file that imports another plugin's module and *also* defines a task with
the same short name -- the shape that broke in production: plugins/nxospinvebringup_slurm.py
does 'from nxpidt.nxospibringup_slurm import config_ixia_license' and then defines its
own checkout_git_branch.

Importing cross_plugin_defs registers its tasks during this file's import, so both
modules land in this plugin file's group. cross_plugin_defs' own reference to
cross_helper must still reach cross_plugin_defs' version, not this one.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask

from nested_defs.cross_plugin_defs import SOME_CONSTANT  # noqa: F401


@app.task(base=FireXTask)
def cross_helper():
    """The higher-precedence version, for everyone outside cross_plugin_defs."""
    pass  # pragma: no cover
