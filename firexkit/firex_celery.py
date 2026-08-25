from typing import Optional, Union
import os

from kombu.utils.objects import cached_property
from celery.app.base import Celery
from celery.backends.redis import RedisBackend
import celery.signals
from celery.utils.log import get_task_logger

from firexkit.broker import handle_broker_timeout
from firexkit.task import FireXTask

logger = get_task_logger(__name__)

_TASK_PRE_RUN_KEY = 'TASK_PRE_RUN'
_TASK_POST_RUN_KEY = 'TASK_POST_RUN'

class FireXCelery(Celery):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backend : RedisBackend

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


def _get_task_info_from_result(
    result: str,
    key: str,
    fx_app: Celery,
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


@celery.signals.task_prerun.connect
def _mark_task_prerun(task, task_id, **_kwargs):
    task.backend.client.hset(task_id, _TASK_PRE_RUN_KEY, 'True')


@celery.signals.task_postrun.connect
def _mark_task_postrun(task, task_id, **_kwargs):
    task.backend.client.hset(task_id, _TASK_POST_RUN_KEY, 'True')