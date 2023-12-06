import os

from importlib import import_module
from celery import bootsteps
from celery.signals import task_postrun
from celery.states import REVOKED, RETRY
from celery.utils.log import get_task_logger
from firexkit.chain import InjectArgs
from firexkit.result import find_unsuccessful_in_chain
from firexkit.result import get_results

from firexapp.application import get_app_tasks
from firexapp.engine.celery import app

logger = get_task_logger(__name__)

root_revoked_backend_key = 'ROOT_REVOKED'

#
# Set flag in backend DB (i.e.: Redis) to indicate root taks has been revoked
#
def backend_set_root_revoked():
     app.backend.set(root_revoked_backend_key, 'True')

#
# Get flag in backend DB (i.e.: Redis) which indicates root taks has been revoked
#
def backend_get_root_revoked():
     return app.backend.get(root_revoked_backend_key)

# noinspection PyPep8Naming
@app.task(bind=True, returns=('chain_results', 'unsuccessful_services'))
def RootTask(self, chain, **chain_args):
    c = InjectArgs(chain=chain, **chain_args)
    for task in get_app_tasks(chain):
        c |= task.s()
    promise = self.enqueue_child(c, block=True, raise_exception_on_failure=False)
    chain_results = get_results(promise)
    unsuccessful_services = find_unsuccessful_in_chain(promise)
    return chain_results, unsuccessful_services


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

    submit_app = kwargs.get("submit_app")
    if not submit_app:
        logger.warning("Cannot find submit_app. Root task post-run handling aborted.")
        return

    result = task.AsyncResult(task_id)
    sync = kwargs.get("sync", False)

    result_state = result.state
    is_revoked = result_state in [REVOKED, RETRY] # Revoked can be in retry state with celery 5.1.0

    if is_revoked:
        backend_set_root_revoked()

    if sync and not is_revoked:
        logger.debug("Sync run has not been revoked. Cleanup skipped.")
        # Only if --sync run was revoked do we want to do the shutdown here; else it's done in firex.py
        return

    # Let this signal cause self-destruct
    submit_app.self_destruct(chain_details=(result, kwargs),
                             reason=f'Root task completion ({result.state}) detected via postrun signal.',
                             run_revoked=is_revoked)

    logger.info("Root task post run signal completed")


class BrokerShutdown(bootsteps.StartStopStep):
    """ This celery shutdown step will cleanup redis """
    label = "Broker"

    # noinspection PyMethodMayBeStatic
    def shutdown(self, parent):
        if parent.hostname.startswith(app.conf.primary_worker_name + "@"):
            # shut down the broker
            from firexapp.broker_manager.broker_factory import BrokerFactory
            BrokerFactory.broker_manager_from_env(logs_dir=app.conf.get('logs_dir')).shutdown()
            logger.debug("Broker shut down from boot step.")
        else:
            logger.debug("Not the primary celery instance. Broker will not be shut down.")


app.steps['consumer'].add(BrokerShutdown)
