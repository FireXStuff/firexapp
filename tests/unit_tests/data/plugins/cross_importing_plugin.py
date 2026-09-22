"""
A plugin file that imports another plugin's module and *also* defines a task with
the same short name -- firex_cisco's plugins/nxospinvebringup_slurm.py does
'from nxpidt.nxospibringup_slurm import config_ixia_license' and then defines its own
checkout_git_branch.

Importing cross_plugin_defs registers its tasks, but only this file was listed as a
plugin, so only this file gets plugin precedence. cross_plugin_defs' version is
therefore overridden by this one, including for cross_plugin_defs' own reference to
it.
"""
from nested_defs.cross_plugin_defs import SOME_CONSTANT  # noqa: F401

from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def cross_helper():
    """The listed plugin's version, which wins over the imported module's."""
    # pragma: no cover
