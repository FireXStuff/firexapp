"""
Process events from Celery.
"""

import abc
import logging
from pathlib import Path
import threading
import traceback
import time
from typing import Optional

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
        self.celery_event_receiver : Optional[EventReceiver] = None

        self.receiver_ready_file : Optional[Path]

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
        while (
            # if the root is not complete, it may be worth retrying to connect
            # to the the broker.
            not self._is_root_complete()

            # Subclasses can stop Celery event receiving by setting this bool,
            # so we don't want to reconnect to the broker.
            and not getattr(self.celery_event_receiver, 'should_stop', False)
        ):
            try:
                try_interval *= 2
                with self.celery_app.connection() as conn:
                    conn.ensure_connection(max_retries=1, interval_start=0)
                    self.celery_event_receiver = EventReceiver(
                        conn,
                        handlers={"*": self._on_event},
                        app=self.celery_app)
                    try_interval = 1
                    self._ready()
                    self.celery_event_receiver.capture(
                        limit=None, timeout=None, wakeup=True)
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
            else:
                logger.debug("Celery receiver stopped")

    def _on_ready(self):
        """Called a single time when the thread is ready to start receiving events."""
        pass

    def _on_event(self, event):
        try:
            self._on_celery_event(event)

            if (self._is_root_complete()
                # In case checking all tests is expensive, check root first. Remaining
                # tasks only need to be checked once root is complete since everything
                # can't be complete if the root is not complete.
                and self._all_tasks_complete()
                and self.celery_event_receiver):
                logger.info("Stopping Celery event receiver because all tasks are complete.")
                self.celery_event_receiver.should_stop = True
        except Exception as e:
            logger.exception(e)
            raise

    @abc.abstractmethod
    def _is_root_complete(self):
        """Return True only when the root task is complete and normal shutdown can occur."""
        pass

    def _all_tasks_complete(self):
        """Return True only when all tasks are complete and the event receiver can be stopped."""
        return False

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
