"""
The lower-precedence half of a three-link override chain: core task <- this plugin
<- top_override_plugin. At runtime this plugin's contribution is executed as the
replacement task 'mid_override_plugin.chained_override_me_orig'.
"""
from firexapp.engine.celery import app
from firexkit.task import FireXTask


@app.task(base=FireXTask)
def chained_override_me():
    """An override that is itself overridden by a higher-precedence plugin."""
    pass  # pragma: no cover
