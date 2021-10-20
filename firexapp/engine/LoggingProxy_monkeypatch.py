# This monkey patch addresses the bug introduced in
# https://github.com/celery/celery/pull/6791/commits/51f5b01df0434144521b23a35d16aebfee08c3ae
# Until the fix in
# https://github.com/celery/celery/pull/6838/files
# gets released in Celery>=5.2.0

from celery.utils import log
from kombu.utils.encoding import safe_str
import sys
from celery.utils.log import _in_sighandler


def monkey_write(self, data):
    # type: (AnyStr) -> int
    """Write message to logging object."""
    if _in_sighandler:
        safe_data = safe_str(data)
        print(safe_data, file=sys.__stderr__)
        return len(safe_data)
    if getattr(self._thread, 'recurse_protection', False):
        # Logger is logging back to this file, so stop recursing.
        return 0
    # This is the fix
    data = data.rstrip('\n')
    if data and not self.closed:
        self._thread.recurse_protection = True
        try:
            safe_data = safe_str(data)
            self.logger.log(self.loglevel, safe_data)
            return len(safe_data)
        finally:
            self._thread.recurse_protection = False
    return 0


def monkeypatch_LogggingProxy():
    log.LoggingProxy.write = monkey_write