"""
Process events from Celery.
"""

import abc
import logging
from pathlib import Path
import threading
import traceback
import time

from celery.app.base import Celery
from celery.events import EventReceiver


logger = logging.getLogger(__name__)


class BrokerEventConsumerThread(threading.Thread):
    """Base class for receiving celery events."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, celery_app: Celery, max_retry_attempts: int = None, receiver_ready_file: str = None):
        threading.Thread.__init__(self)
        self.celery_app = celery_app
        self.max_try_interval = 2**max_retry_attempts if max_retry_attempts is not None else 32
        self.ready = False

        if receiver_ready_file:
            self.receiver_ready_file = Path(receiver_ready_file)
            assert not self.receiver_ready_file.exists(), \
                "Receiver ready file must not already exist: %s." % self.receiver_ready_file
        else:
            self.receiver_ready_file = None

    def _ready(self):
        if not self.ready:
            if self.receiver_ready_file:
                self.receiver_ready_file.touch()
            self._on_ready()
            self.ready = True

    def run(self):
        self._run_from_broker()

    def _run_from_broker(self):
        """Load the events from celery"""
        try:
            self._capture_events()
        finally:
            self._on_cleanup()

    def _capture_events(self):
        try_interval = 1
        while not self._is_root_complete():
            try:
                try_interval *= 2
                with self.celery_app.connection() as conn:
                    conn.ensure_connection(max_retries=1, interval_start=0)
                    recv = EventReceiver(conn,
                                         handlers={"*": self._on_event},
                                         app=self.celery_app)
                    try_interval = 1
                    self._ready()
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                logger.exception("Received external shutdown.")
                self._on_external_shutdown()
            # pylint: disable=C0321
            except Exception:
                if self._is_root_complete():
                    logger.info("Root task complete; stopping broker receiver thread.")
                    return
                logger.error(traceback.format_exc())
                if try_interval > self.max_try_interval:
                    logger.warning("Maximum broker retry attempts exceeded, stopping receiver thread)."
                                   " Will no longer retry despite incomplete root task.")
                    return
                logger.debug("Try interval %d secs, still worth retrying." % try_interval)
                time.sleep(try_interval)

    def _on_ready(self):
        """Called a single time when the thread is ready to start receiving events."""
        pass

    def _on_event(self, event):
        try:
            self._on_celery_event(event)
        except Exception as e:
            logger.exception(e)
            raise

    @abc.abstractmethod
    def _is_root_complete(self):
        """Return True only when the root task is complete and normal shutdown can occur."""
        pass

    @abc.abstractmethod
    def _on_celery_event(self, event):
        """Callback invoked when a event is received from Celery."""
        pass

    def _on_external_shutdown(self):
        """Callback invoked when the thread is shutdown externally (e.g. signal)"""
        pass

    def _on_cleanup(self):
        """Callback invoked when the receiver has stopped listening, for any reason."""
        pass
