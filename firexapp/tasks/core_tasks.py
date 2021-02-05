from celery.utils.log import get_task_logger
from firexkit.chain import FireXTask
from firexkit.task import flame

from firexapp.engine.celery import app

logger = get_task_logger(__name__)

# noinspection PyPep8Naming
@app.task(bind=True, returns=FireXTask.DYNAMIC_RETURN)
@flame('status')
def CopyBogKeys(self: FireXTask, bog_key_map: dict, strict: bool = False):
    """
    This service copies selected keys from this FireXTask instance's bog into new keys with a different name and
    returns the resulting dictionary. It can therefore be used to preserve values in a chain when they will
    be trampled by return values from downstream in the chain.

    :param bog_key_map: mapping from existing key names to new key names that existing bog values should be copied to
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
