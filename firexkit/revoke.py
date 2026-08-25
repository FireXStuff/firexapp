import os
import json
from pathlib import Path

from celery import current_app
from datetime import timedelta, datetime
from celery.utils.log import get_task_logger
from firexkit.inspect import get_revoked

logger = get_task_logger(__name__)


class RevokedRequests(object):
    """
     Need to inspect the app for the revoked requests, because AsyncResult.state of a task that hasn't
    been de-queued and executed by a worker but was revoked is PENDING (i.e., the REVOKED state is only updated upon
    executing a task). This phenomenon makes the wait_for_results wait on such "revoked" tasks, and therefore
    required us to implement this work-around.
    """

    _instance = None

    @classmethod
    def instance(cls, existing_instance=None):
        if existing_instance is not None:
                cls._instance = existing_instance
        if cls._instance is None:
            cls._instance = RevokedRequests()
        return cls._instance

    def __init__(self, timer_expiry_secs=60, skip_first_cycle=True):
        self.timer_expiry = timedelta(seconds=timer_expiry_secs)
        self.revoked_list = []
        self.last_updated = datetime.utcnow() if skip_first_cycle else None

    @classmethod
    def get_revoked_list_from_app(cls):
        revoked_list = list()
        v = get_revoked(retry_if_None_returned=False,
                        timeout=60,
                        destination=(f'mc@{current_app.conf.mc}', ))
        if not v:
            return revoked_list
        else:
            v = v.values()
        if v:
            for l in v:
                revoked_list += l
        return revoked_list

    def update(self, verbose=False):
        self.revoked_list = self.get_revoked_list_from_app()
        self.last_updated = datetime.utcnow()
        if verbose:
            logger.debug('RevokedRequests list updated at %s to %r' % (self.last_updated, self.revoked_list))

    def _task_in_revoked_list(self, result_id):
        if self.last_updated is None:
            self.update()
        return result_id in self.revoked_list

    def is_revoked(self, result_id, timer_expiry_secs=None):
        if self._task_in_revoked_list(result_id):
            return True
        else:
            # Updating the revoked_list is an expensive operation, so only do it periodically
            timer_expiry = self.timer_expiry if timer_expiry_secs is None else \
                timedelta(seconds=timer_expiry_secs)
            time_lapsed = datetime.utcnow()-self.last_updated
            if time_lapsed > timer_expiry:
                self.update()
                return self._task_in_revoked_list(result_id)
            else:
                return False


def get_chain_head(parent, child):
    if child == parent or parent is None or child is None:
        return child
    one_up = child.parent
    if one_up == parent or one_up is None:
        return child
    else:
        return get_chain_head(parent=parent, child=one_up)


def revoke_nodes_up_to_parent(starting_node, parent):
    from firexkit.result import get_result_logging_name
    node = starting_node
    parent_name = get_result_logging_name(parent)
    while node != parent:
        one_up = node.parent
        logger.info('Revoking child %r of parent %r' % (get_result_logging_name(node), parent_name))
        node.revoke(terminate=True)
        node = one_up

