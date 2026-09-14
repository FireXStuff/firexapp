"""
    Run-relative time limits.

    A FireX run has a total time budget -- ``run_soft_time_limit``, seeded from the
    ``--soft_time_limit`` submit argument. Tasks that must run for "nearly the whole run"
    express that as a :class:`RunTimeReserve`: all the time remaining in the run, less a
    reserve left over for cleanup and post-processing of the long-running work.

    The same object is accepted everywhere a number of seconds is, so one concept covers
    every shape the need takes::

        self.enqueue_child(Child.s(), soft_time_limit=RunTimeReserve(60*60))
        @app.task(run_time_limit_reserve=60*60)
        ars.wait_for_all(max_wait=RunTimeReserve(60*60))
        RunTimeReserve(60*60).resolve()  # a plain number, e.g. for a child run's CLI

    Resolution is always deferred to the moment of use, so a run budget that is increased
    mid-run (see ``FireXTask.ensure_run_time_remaining``) is picked up without any
    signalling.
"""
import time
from dataclasses import dataclass

from celery.app import app_or_default
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# A RunTimeReserve never resolves to zero or less. A task starting near the end of the run
# still needs a usable limit, and celery treats a soft_time_limit of 0 as "no limit" --
# which would be the opposite of what a nearly-exhausted budget should mean.
DEFAULT_MINIMUM_RUN_TIME_REMAINING = 60


def get_run_start_time(app=None) -> float:
    """
        Epoch seconds at which this run started, parsed from the FireX ID.

        Deliberately not a broker read: the FireX ID is in every process's config, and this
        is the same anchor firex_cisco's run_info.is_after_max_run_end_time() already uses,
        so the deadline derived here agrees with the bundle's existing view of a run's end.
    """
    # Imported lazily: firexapp.submit.uid imports firexkit, and firexkit.task imports this
    # module very early (celery instantiates task_cls during app setup).
    from firexapp.submit.uid import FireXIdParts

    app = app or app_or_default()
    return FireXIdParts.from_str(app.conf.fx_env.firex_id).timestamp.timestamp()


def get_run_soft_time_limit(app=None) -> float:
    """The run's current total time budget, in seconds."""
    app = app or app_or_default()
    getter = getattr(app, 'get_run_soft_time_limit', None)
    if getter is None:
        # Not a FireXCelery (e.g. a bare app in a unit test); fall back to celery's
        # per-task default, which is what the budget is seeded from anyway.
        return app.conf.task_soft_time_limit
    return getter()


def get_run_deadline(app=None) -> float:
    """Epoch seconds at which this run's time budget is exhausted."""
    app = app or app_or_default()
    return get_run_start_time(app) + get_run_soft_time_limit(app)


@dataclass(frozen=True)
class RunTimeReserve:
    """
        A time limit meaning "all the run time remaining, less :attr:`reserve` seconds".

        Args:
            reserve: seconds to leave unused at the end of the run, for cleanup and
                post-processing of whatever this limit applies to.
            minimum: floor for :meth:`resolve`, so a task starting late still gets a
                usable limit rather than zero.
    """

    reserve: float = 0
    minimum: float = DEFAULT_MINIMUM_RUN_TIME_REMAINING

    def remaining(self, app=None) -> float:
        """
            Seconds from now until the run's deadline, less :attr:`reserve`.

            Goes negative once the run is past that point, unlike :meth:`resolve`. Use
            this where running out of time is meaningful -- a wait that should give up --
            and :meth:`resolve` where a number has to be usable as a time limit.
        """
        return get_run_deadline(app) - time.time() - self.reserve

    def resolve(self, app=None) -> float:
        """
            :meth:`remaining`, floored at :attr:`minimum`, as a soft time limit.

            A task enqueued near the end of the run still needs a usable limit, and
            celery reads a soft_time_limit of zero as no limit at all.
        """
        return max(self.remaining(app), self.minimum)

    def __str__(self):
        return f'the run time remaining less {self.reserve}s'


def describe_wait(max_wait: float | RunTimeReserve | None) -> str:
    """How a max_wait reads in a timeout message."""
    if isinstance(max_wait, RunTimeReserve):
        return str(max_wait)
    return f'{max_wait} seconds'


def resolve_remaining_wait(
    max_wait: float | RunTimeReserve | None,
    start_time: float,
    now: float,
    app=None,
) -> float | None:
    """
        Seconds left of ``max_wait``, or :const:`None` if the wait is unbounded.

        ``start_time`` and ``now`` must come from the same clock; which clock is irrelevant,
        since a RunTimeReserve is absolute and a number is relative to ``start_time``.

        Call this on every poll iteration rather than once up front: that is what lets an
        already-blocked wait extend itself when the run budget is increased.
    """
    if not max_wait and not isinstance(max_wait, RunTimeReserve):
        # Preserves the long-standing convention that a falsy max_wait means no limit.
        # RunTimeReserve(0) is excluded: it means "the whole rest of the run".
        return None
    if isinstance(max_wait, RunTimeReserve):
        # remaining(), not resolve(): a wait that is out of run time has to give up,
        # rather than be handed the minimum again on every poll and never expire.
        return max_wait.remaining(app)
    return (start_time + max_wait) - now
