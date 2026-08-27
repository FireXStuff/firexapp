"""
Process events from Celery in to flame data model.
"""

import logging
from pathlib import Path
import threading
import traceback
import json
import time
from typing import Optional
from io import TextIOWrapper
from datetime import datetime, timezone, timedelta

from celery.events import EventReceiver


from firex_flame.controller import FlameAppController
from firex_flame.flame_helper import BrokerConsumerConfig

logger = logging.getLogger(__name__)


def _get_event_lag(event_timestamp, celery_utcoffset):
    # celery_utcoffset is garbage. It's the UTC offset, but
    # event_timestamp is not in this timezone. It looks like Celery
    # assumes the mc is in UTC, so the real offset is mc_offset_hr + celery_utcoffset
    try:
        now_dt = datetime.now(timezone.utc)
        mc_offset_hr = now_dt.astimezone().utcoffset().total_seconds() / 3600
        real_tz_offset_hr = mc_offset_hr + celery_utcoffset
        event_datetime = datetime.fromtimestamp(
            event_timestamp,
            timezone(timedelta(hours=real_tz_offset_hr))
        )
        return (now_dt - event_datetime.replace(tzinfo=timezone.utc)).total_seconds()
    except Exception:
        return None


def _log_event_received(event, event_count):
    lag_msg = ''
    event_timestamp = event.get('timestamp')

    if event_timestamp:
        lag = _get_event_lag(event_timestamp, event.get('utcoffset', 0))
        if lag is not None:
            lag_msg = f' (lag: {lag:.2f})'

    logger.debug(
        f'Received Celery event number {event_count}'
        f' with task uuid: {event.get("uuid")}{lag_msg}')


class BrokerEventConsumerThread(threading.Thread):
    """Events threading class"""

    def __init__(
        self,
        celery_app,
        flame_controller: FlameAppController,
        config: BrokerConsumerConfig,
        recording_file: str,
        shutdown_handler,
    ):
        threading.Thread.__init__(self, daemon=True)
        self.celery_app = celery_app

        self.open_recording_file : Optional[TextIOWrapper]
        if recording_file:
            self.open_recording_file = open(recording_file, "a", encoding="utf-8")
        else:
            self.open_recording_file = None

        self.flame_controller = flame_controller
        self.max_try_interval = 2**config.max_retry_attempts if config.max_retry_attempts is not None else 32
        self.terminate_on_complete = config.terminate_on_complete
        self.stopped_externally = False
        self.shutdown_handler = shutdown_handler
        self._event_count = 0
        self.celery_event_receiver : Optional[EventReceiver] = None

        self.receiver_ready_file : Optional[Path]

        if config.receiver_ready_file:
            self.receiver_ready_file = Path(config.receiver_ready_file)
            assert not self.receiver_ready_file.exists(), \
                f"Receiver ready file must not already exist: {self.receiver_ready_file}"
        else:
            self.receiver_ready_file = None
        self.is_first_receive = True

    def _create_receiver_ready_file(self):
        if self.receiver_ready_file:
            self.receiver_ready_file.touch()

    def run(self):
        """Listen for events from celery"""
        try:
            self._capture_events()
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            self.flame_controller.finalize_all_tasks()
            if self.open_recording_file:
                self.open_recording_file.close()
                self.open_recording_file = None
        except Exception as ex:
            logger.error("Failed to cleanup during receiver completion.")
            logger.exception(ex)
        finally:
            logger.info("Completed receiver cleanup.")
            if self.terminate_on_complete and not self.stopped_externally:
                self.shutdown_handler.shutdown("Terminating on completion, as requested by input args.")

    def _capture_events(self):
        try_interval = 1
        while not self.flame_controller.is_root_complete():
            try:
                try_interval *= 2
                with self.celery_app.connection() as conn:
                    conn.ensure_connection(max_retries=1, interval_start=0)
                    self.celery_event_receiver = EventReceiver(
                        conn,
                        handlers={"*": self._safe_on_celery_event},
                        app=self.celery_app)
                    try_interval = 1

                    self.celery_event_receiver.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit) as e:
                self.stopped_externally = True
                self.shutdown_handler.shutdown(str(e))
            except Exception: #noqa
                if self.flame_controller.is_root_complete():
                    logger.info("Root task complete; stopping broker receiver (not entire server).")
                    return
                if self.shutdown_handler.shutdown_received:
                    self.stopped_externally = True
                    logger.info("Shutdown handler received shutdown request; stopping broker receiver.")
                    return
                logger.error(traceback.format_exc())
                if try_interval > self.max_try_interval:
                    logger.warning("Maximum broker retry attempts exceeded, stopping receiver (not entire server)."
                                   " Will no longer retry despite incomplete root task.")
                    return
                logger.debug(f"Try interval {try_interval} secs, still worth retrying.")
                time.sleep(try_interval)

    def _safe_on_celery_event(self, event):
        try:
            self._on_celery_event(event)
        except Exception:
            logger.exception(f'Failed to process Celery event: {event}')

    def _on_celery_event(self, event):
        """Callback function for when an event is received

        Arguments:
            event(dict): The event to aggregate and send downstream.
        """
        if self.is_first_receive:
            # Hopefully this is 'flame-indicate-ready' event
            self._create_receiver_ready_file()
            self.is_first_receive = False

        # Append the event to the recording file if it is specified
        if self.open_recording_file:
            # the rec file is just for the record, doesn't matter
            _safe_dump_json_line(event, self.open_recording_file)

        if self._event_count % 100 == 0:
            if self.open_recording_file:
                _safe_flush(self.open_recording_file) # the rec file is just for the record, doesn't matter
            _log_event_received(event, self._event_count)
        self._event_count += 1


        self.flame_controller.update_graph_and_sio_clients([event])

        if (
            self.flame_controller.is_all_tasks_complete()
            and self.celery_event_receiver
        ):
            logger.info("Stopping Celery event receiver because all tasks are complete.")
            self.celery_event_receiver.should_stop = True


def _safe_dump_json_line(data, open_file: TextIOWrapper):
    try:
        json.dump(data, open_file)
        open_file.write("\n")
    except OSError:
        pass

def _safe_flush(open_file: TextIOWrapper):
    try:
        open_file.flush()
    except OSError:
        pass