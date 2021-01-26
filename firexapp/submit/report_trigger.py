from firexkit.argument_conversion import ConverterRegister
from firexapp.engine.celery import app
from firexapp.tasks.core_tasks import get_configured_root_task
from firexapp.submit.reporting import ReportersRegistry
from firexkit.task import flame_collapse


__all__ = ["RunInitialReport"]


@ConverterRegister.register_for_task(get_configured_root_task())
def run_initial_reporting(kwargs):
    # only run task if necessary; generators implement pre-load overloads
    generators = ReportersRegistry.get_generators()
    generators_with_pre_run = [g for g in generators if g.__class__.__dict__.get("pre_run_report")]
    if not generators_with_pre_run:
        return

    RunInitialReport.s(**kwargs).enqueue(block=False)


# noinspection PyPep8Naming
@app.task
@flame_collapse('self')
def RunInitialReport(**kwargs):
    ReportersRegistry.pre_run_report(kwargs)
