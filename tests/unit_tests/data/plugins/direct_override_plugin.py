"""
A plugin file that genuinely overrides indirect_override_me, listed *before*
indirect_override_plugin.py.

Its whole purpose is to be outranked if a module that a later plugin merely imports
(indirect_override_defs, pulled in by indirect_override_plugin.py) is ever given that
plugin's precedence.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def indirect_override_me():
    """The override that must survive a later plugin importing a same-named task."""
    pass  # pragma: no cover
