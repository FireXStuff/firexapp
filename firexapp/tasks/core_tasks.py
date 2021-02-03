import os

from importlib import import_module
from celery import bootsteps
from celery.signals import task_postrun
from celery.states import REVOKED
from celery.utils.log import get_task_logger
from firexkit.chain import InjectArgs, FireXTask
from firexkit.result import get_results_upto_parent, find_unsuccessful_in_chain
from firexkit.task import flame

from firexapp.application import get_app_tasks
from firexapp.engine.celery import app

logger = get_task_logger(__name__)


# noinspection PyPep8Naming
@app.task(bind=True, returns=('chain_results', 'unsuccessful_services'))
def RootTask(self, chain, **chain_args):
    c = InjectArgs(chain=chain, **chain_args)
    for task in get_app_tasks(chain):
        c |= task.s()
    promise = self.enqueue_child(c, block=True, raise_exception_on_failure=False)
    chain_results = get_results_upto_parent(promise)
    unsuccessful_services = find_unsuccessful_in_chain(promise)
    return chain_results, unsuccessful_services


# noinspection PyPep8Naming
@app.task(bind=True, returns=FireXTask.DYNAMIC_RETURN)
@flame('status')
def CopyBogKeys(self: FireXTask, bog_key_map: dict, strict: bool = False):
    """
    This service copies selected keys from this FireXTask instance's bog into new keys with a different name and
    returns the resulting dictionary.

    :param bog_key_map: mapping from existing key names to new key names that existing values should be copied to
        in the return value.
    :param strict: [True|False] (default=False):
        True: All source entries specified in the mapping must exist in the  bog or the service fails.
        False: Skip source entries that don't exist in bog.
    :return: dict with keys from the values of bog_key_map, and values from the BoG.
    """

    logger.debug('abog content: %r' % self.abog)
    flame_status = ""

    new = {}
    for existing_bog_key, new_key in bog_key_map.items():
        try:
            existing_value = self.abog[existing_bog_key]
        except KeyError:
            if strict:
                raise AssertionError(f'Strict is specified and no entry found for "{existing_bog_key}" in bog.')
            logger.debug(f'No entry for "{existing_bog_key}" in bog. Skipping...')
        else:
            new[new_key] = existing_value
            status_str = f'{existing_bog_key}={new_key}'
            logger.debug('BOG mapping: ' + status_str)
            if flame_status:
                flame_status += '<BR>'
            flame_status += status_str
            self.send_firex_html(status=flame_status)

    return new


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

    if sync and result.state != REVOKED:
        logger.debug("Sync run has not been revoked. Cleanup skipped.")
        # Only if --sync run was revoked do we want to do the shutdown here; else it's done in firex.py
        return

    # Let this signal cause self-destruct
    submit_app.self_destruct(chain_details=(result, kwargs),
                             reason=f'Root task completion ({result.state}) detected via postrun signal.')

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
            BrokerFactory.broker_manager_from_env().shutdown()
            logger.debug("Broker shut down from boot step.")
        else:
            logger.debug("Not the primary celery instance. Broker will not be shut down.")


app.steps['consumer'].add(BrokerShutdown)
