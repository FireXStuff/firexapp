"""
A plugin file listed after reimported_defs/reimported_plugin.py that both imports it
by package path -- executing it a second time, under a second module name -- and
overrides the task it defines.

Mirrors firex_cisco's prio1/bazel_pr_ops_mgr_tests.py doing
'from ci_plugins import bazel_pr_ops' and then overriding _SelectPrOps.
"""

from reimported_defs import reimported_plugin  # noqa: F401

from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def reimported_helper():
    """The override, which must win over both copies of the reimported module."""
    # pragma: no cover
