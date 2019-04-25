import os

from importlib import import_module
from celery import bootsteps
from celery.signals import task_postrun
from celery.utils.log import get_task_logger

from firexkit.chain import InjectArgs
from firexapp.application import get_app_tasks
from firexapp.engine.celery import app

logger = get_task_logger(__name__)


# noinspection PyPep8Naming
@app.task(bind=True)
def RootTask(self, chain, **chain_args):
    c = InjectArgs(chain=chain, **chain_args)
    for task in get_app_tasks(chain):
        c |= task.s()
    self.enqueue_child(c, block=True)


def get_configured_root_task():
    # determine the configured root_task
    root_task_long_name = app.conf.get("root_task")
    root_module_name, root_task_name = os.path.splitext(root_task_long_name)
    if root_module_name != __name__:
        # Root task has been overridden
        root_task_name = root_task_name.lstrip(".")
        root_module = import_module(root_module_name)
        return getattr(root_module, root_task_name)
    else:
        # default FireXApp root task
        return RootTask


# noinspection PyUnusedLocal
@task_postrun.connect(sender=get_configured_root_task())
def handle_firex_root_completion(sender, task, task_id, args, kwargs, **do_not_care):
    logger.info("Root task completed.")
    if kwargs.get("sync", False):
        return

    # Let this signal cause self-destruct if --sync was not specified
    submit_app = kwargs.get("submit_app")
    if submit_app:
        submit_app.self_destruct(chain_details=(task.AsyncResult(task_id), kwargs))
    else:
        logger.warning("self_destruct was not run.")
    logger.info("Root task post run signal completed")


class BrokerShutdown(bootsteps.StartStopStep):
    """ This celery shutdown step will cleanup redis """
    label = "Broker"

    # noinspection PyMethodMayBeStatic
    def shutdown(self, parent):
        from firexapp.submit.submit import SubmitBaseApp
        if parent.hostname.startswith(SubmitBaseApp.PRIMARY_WORKER_NAME + "@"):
            # shut down the broker
            from firexapp.broker_manager.broker_factory import BrokerFactory
            BrokerFactory.get_broker_manager().shutdown()
        else:
            logger.debug("Not the primary celery instance. Broker will not be shut down.")


app.steps['consumer'].add(BrokerShutdown)
