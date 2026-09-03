from typing import Optional, Union
import os

from kombu.utils.objects import cached_property
from celery.app.base import Celery
from celery.backends.redis import RedisBackend
import celery.signals
from celery.utils.log import get_task_logger
from celery.worker.consumer import Consumer
from celery.loaders.base import BaseLoader

from firexapp.events.model import RunStates
from firexapp.engine.firex_revoke import RevokeDetails
from firexapp.broker_manager.broker_factory import RedisManager
from firexkit.broker import handle_broker_timeout
from firexkit.task import FireXTask, convert_to_serializable
from celery.app import app_or_default

logger = get_task_logger(__name__)

_TASK_PRE_RUN_KEY = 'TASK_PRE_RUN'
_TASK_POST_RUN_KEY = 'TASK_POST_RUN'

class FireXCelery(Celery):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def celery_no_tasks_app_from_logs_dir(
        cls,
        logs_dir: str,
    ) -> 'FireXCelery':
        """
            This app instance is suitable for contexts like TrackingServices
            where no tasks will be executed and no bound/unbound tasks will
            be queried for. Inspect queries should still work.

            Task definition loading is disabled for performance purposes.
        """
        broker_url = RedisManager.get_broker_url_from_logs_dir(logs_dir)
        cel_app =  FireXCelery(
            broker=broker_url,
            backend=broker_url,
            accept_content=['pickle', 'json'],
            autofinalize=False,
            set_as_current=False,
            loader=_DisabledTasksLoader,
        )
        # try to stop Celery from doing scanning magic with "empty"
        # config_source and finalized=True
        cel_app.config_from_object(
            dict(
                firex_id=os.path.basename(logs_dir),
                logs_dir=logs_dir,
            )
        )
        cel_app.finalized = True
        cel_app._config_source = None
        return cel_app

    @property
    def backend(self) -> RedisBackend:
        return super().backend

    @cached_property
    def AsyncResult(self):
        from firexkit.result import FxAsyncResult
        return self.subclass_with_self(
            FxAsyncResult,
            name='AsyncResult',
            reverse='AsyncResult',
        )

    def task(self, *args, **kwargs) -> FireXTask:
        return super().task(*args, **kwargs)

    def import_microservices(
        self,
        plugins_files: Union[None, str, list[str]]=None,
        imports: Optional[tuple[str,...]]=None,
    ) -> tuple[
        dict[str, FireXTask],
        dict[str, str]
    ]:
        from firexapp.plugins import (
            convert_plugins_to_list,
            cdl2list,
            load_plugin_modules,
        )

        original_plugins = convert_plugins_to_list(plugins_files)
        resolved_plugins = cdl2list(original_plugins)

        # Create mapping from original plugin paths to resolved full paths
        plugin_path_mapping = {}
        # Build the mapping and validate files exist
        for original, resolved in zip(original_plugins, resolved_plugins):
            if not os.path.isfile(resolved):
                raise FileNotFoundError(resolved)
            plugin_path_mapping[original] = resolved

        if not imports:
            imports = self.conf.imports

        for module_name in imports:
            __import__(module_name)

        load_plugin_modules(plugins_files)

        return self.tasks, plugin_path_mapping

    def task_id_has_prerun(self, task_id: str) -> bool:
        try:
            return bool(
                _get_task_info_from_result(
                    result=task_id,
                    key=_TASK_PRE_RUN_KEY,
                    fx_app=self,
                )
            )
        except AttributeError:
            logger.info('Broker does not support prerun info; probably a dummy broker. Defaulting to prerun=False')

        return False

    def task_id_has_postrun(self, task_id: str) -> bool:
        try:
            return bool(
                _get_task_info_from_result(
                    task_id,
                    key=_TASK_POST_RUN_KEY,
                    fx_app=self,
                )
            )
        except AttributeError:
            logger.info(f'Broker doesn\'t support postrun info; probably a dummy broker. Defaulting to postrun=True')
        return True

    @staticmethod
    def app_or_default() -> 'FireXCelery':
        return app_or_default()


class _DisabledTasksLoader(BaseLoader):
    """
        Celery does a lot of crazy stuff automatically, try
        to disable module scanning.
    """

    def autodiscover_tasks(self, *args, **kwargs):
        pass

    def _smart_import(self, *args, **kwargs):
        return {}


def _get_task_info_from_result(
    result: str,
    key: str,
    fx_app: FireXCelery,
) -> str:
    key_value_bytes : Optional[bytes] = handle_broker_timeout(
        fx_app.backend.client.hget,
        args=(result, key),
    )
    if key_value_bytes is None:
        info = ''
    else:
        info = key_value_bytes.decode()
    return info


@celery.signals.task_postrun.connect
def _mark_task_postrun(task: FireXTask, task_id: str, **_kwargs):
    task.backend.client.hset(task_id, _TASK_POST_RUN_KEY, 'True')


@celery.signals.before_task_publish.connect
def _populate_task_info(sender: str, declare, headers, **_kwargs):
    task_info = {'name': sender}
    try:
        task_info['queue'] = declare[0].name
    except (IndexError, AttributeError):
        pass

    FireXCelery.app_or_default().backend.client.hmset(
        headers['id'],
        task_info,
    )


@celery.signals.task_prerun.connect
def _update_task_name(sender: FireXTask, task_id: str, *_args, **_kwargs):
    sender.backend.client.hset(task_id, _TASK_PRE_RUN_KEY, 'True')
    FireXTask.set_backend_task_start_time(task_id)
    # Although the name was populated in _populate_task_info before_task_publish, the name
    # can be inaccurate if it was a plugin. We can only over-write it with the accurate name
    # at task_prerun.
    handle_broker_timeout(
        sender.app.backend.client.hset,
        args=(task_id, 'name', sender.name),
        timeout=5*60,
        reraise_on_timeout=False,
    )


@celery.signals.worker_ready.connect()
def _celery_worker_ready(sender: Consumer, **_kwargs):
    queue_names = [queue.name for queue in sender.task_consumer.queues]
    if queue_names:
        sender.app.backend.client.sadd('QUEUES', *queue_names)


@celery.signals.task_postrun.connect()
def statsd_task_postrun(
    sender: FireXTask,
    task: FireXTask,
    task_id: str,
    *_args,
    **donotcare,
):
    # Celery can send task-revoked event before task is completed, allowing other states (e.g. task-unblocked) to
    # be emitted after task-revoked. Sending another indicator of revoked here allows the terminal state to be
    # correctly captured by listeners, since task_postrun occurs when the task is _really_ complete.
    if task.AsyncResult(task_id).fx_is_revoked():
        try:
            RevokeDetails.write_task_revoke_complete(
                sender.app.conf.logs_dir,
                task_id,
            )
        except Exception as e:
            logger.warning(f'Failed to write revoke complete for task {task_id}: {e}')
        task.send_event(RunStates.REVOKE_COMPLETED.to_celery_event_type())

    _send_task_completed_event(task)


@celery.signals.task_revoked.connect()
def statsd_task_revoked(sender: FireXTask, request=None, *_args, **_kwargs):
    # sender.request doesn't necessarily refer to this task: task_revoked is sent
    # from the worker's controlling process (not the process that ran the task),
    # so push the actual revoked task's context to get its correct id/duration.
    if request:
        sender.request_stack.push(request)
    try:
        _send_task_completed_event(sender)
    finally:
        if request:
            sender.request_stack.pop()


def _send_task_completed_event(task: Optional[FireXTask]):
    if task:
        if ( actual_runtime := task.duration() ) is not None:
            task.send_event(
                'task-completed',
                actual_runtime=convert_to_serializable(
                    max(actual_runtime, 0)
                )
            )