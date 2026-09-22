"""
A plugin file that is also imported, by package path, by a plugin listed after it.

_get_plugin_module_name() derives a module name from the basename alone, so listing
this file imports it as 'reimported_plugin' while reimporting_plugin.py's
'from reimported_defs import reimported_plugin' executes it a second time as
'reimported_defs.reimported_plugin'. Both copies of every task below must be
overridden by the plugin listed last.

This is the shape of firex_cisco's ci_plugins/bazel_pr_ops.py, which
prio1/bazel_pr_ops_mgr_tests.py both imports and overrides.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def reimported_helper():
    """Exists once per copy of this module, and is overridden in both."""
    # pragma: no cover


@app.task(base=FireXTask)
def task_using_reimported_helper():
    """References reimported_helper from the module that defines it."""
    return reimported_helper.s()
