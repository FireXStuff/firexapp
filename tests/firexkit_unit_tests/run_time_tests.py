import datetime
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pytz

from firexapp.submit.uid import firex_id_str
from firexkit.run_time import (
    DEFAULT_MINIMUM_RUN_TIME_REMAINING,
    RunTimeReserve,
    describe_wait,
    get_run_deadline,
    get_run_start_time,
    resolve_remaining_wait,
)

_RUN_START = datetime.datetime(2026, 9, 14, 12, 0, 0, tzinfo=pytz.utc)
_RUN_START_EPOCH = _RUN_START.timestamp()


def _create_app(run_soft_time_limit=10 * 60 * 60, run_start=_RUN_START):
    """A stand-in for FireXCelery holding just what run_time reads."""
    return SimpleNamespace(
        conf=SimpleNamespace(
            fx_env=SimpleNamespace(firex_id=firex_id_str("someuser", run_start, 1234)),
            task_soft_time_limit=72 * 60,
            run_soft_time_limit=run_soft_time_limit,
        ),
        get_run_soft_time_limit=lambda: run_soft_time_limit,
    )


class RunStartTests(unittest.TestCase):
    def test_run_start_comes_from_the_firex_id(self):
        self.assertEqual(_RUN_START_EPOCH, get_run_start_time(_create_app()))

    def test_deadline_is_run_start_plus_the_budget(self):
        self.assertEqual(
            _RUN_START_EPOCH + 3600,
            get_run_deadline(_create_app(run_soft_time_limit=3600)),
        )

    def test_falls_back_to_task_soft_time_limit_without_a_firex_celery(self):
        app = _create_app()
        del app.get_run_soft_time_limit  # e.g. a bare Celery in a unit test.

        self.assertEqual(
            _RUN_START_EPOCH + app.conf.task_soft_time_limit,
            get_run_deadline(app),
        )


class RunTimeReserveTests(unittest.TestCase):
    def resolve(self, reserve, elapsed, budget=10 * 60 * 60, **kwargs):
        app = _create_app(run_soft_time_limit=budget)
        with patch("time.time", return_value=_RUN_START_EPOCH + elapsed):
            return RunTimeReserve(reserve, **kwargs).resolve(app)

    def test_resolves_to_the_remaining_run_less_the_reserve(self):
        # 3h into a 10h run, keeping an hour back: 10 - 3 - 1.
        self.assertEqual(6 * 60 * 60, self.resolve(60 * 60, elapsed=3 * 60 * 60))

    def test_decays_as_the_run_proceeds(self):
        self.assertGreater(
            self.resolve(0, elapsed=60),
            self.resolve(0, elapsed=3600),
        )

    def test_a_raised_budget_resolves_to_more_time(self):
        self.assertEqual(
            5 * 60 * 60,
            self.resolve(60 * 60, elapsed=60 * 60, budget=7 * 60 * 60),
        )
        # The same task, resolved again after the budget was raised by 3h.
        self.assertEqual(
            8 * 60 * 60,
            self.resolve(60 * 60, elapsed=60 * 60, budget=10 * 60 * 60),
        )

    def test_never_resolves_below_the_minimum(self):
        # Past the deadline entirely; celery reads a limit of 0 as "no limit".
        self.assertEqual(
            DEFAULT_MINIMUM_RUN_TIME_REMAINING,
            self.resolve(60 * 60, elapsed=20 * 60 * 60),
        )

    def test_remaining_goes_negative_past_the_deadline(self):
        app = _create_app()
        with patch("time.time", return_value=_RUN_START_EPOCH + 20 * 60 * 60):
            self.assertEqual(-11 * 60 * 60, RunTimeReserve(60 * 60).remaining(app))

    def test_honours_an_explicit_minimum(self):
        self.assertEqual(
            30,
            self.resolve(0, elapsed=20 * 60 * 60, minimum=30),
        )


class ResolveRemainingWaitTests(unittest.TestCase):
    def test_plain_max_wait_counts_down_from_the_start(self):
        self.assertEqual(
            40,
            resolve_remaining_wait(100, start_time=1000, now=1060),
        )

    def test_no_max_wait_is_unbounded(self):
        self.assertIsNone(resolve_remaining_wait(None, start_time=1000, now=1060))

    def test_falsy_max_wait_stays_unbounded(self):
        # Long-standing convention: `if max_wait and ...`.
        self.assertIsNone(resolve_remaining_wait(0, start_time=1000, now=1060))

    def test_zero_reserve_is_a_real_limit(self):
        # Unlike 0, RunTimeReserve(0) means "the whole rest of the run".
        with patch("time.time", return_value=_RUN_START_EPOCH + 3600):
            self.assertEqual(
                9 * 60 * 60,
                resolve_remaining_wait(
                    RunTimeReserve(0),
                    start_time=1000,
                    now=1060,
                    app=_create_app(),
                ),
            )

    def test_run_relative_wait_ignores_the_callers_clock(self):
        # start_time/now are irrelevant: the deadline is the run's, not the wait's.
        with patch("time.time", return_value=_RUN_START_EPOCH + 3600):
            remaining = resolve_remaining_wait(
                RunTimeReserve(60 * 60),
                start_time=0,
                now=10**9,
                app=_create_app(),
            )
        self.assertEqual(8 * 60 * 60, remaining)

    def test_a_wait_past_the_run_deadline_is_out_of_time(self):
        # resolve()'s minimum floor must not apply here, or the wait would be handed
        # another minute on every poll and never expire.
        with patch("time.time", return_value=_RUN_START_EPOCH + 20 * 60 * 60):
            remaining = resolve_remaining_wait(
                RunTimeReserve(60 * 60),
                start_time=0,
                now=0,
                app=_create_app(),
            )
        self.assertLess(remaining, 0)

    def test_describes_both_kinds_of_wait_for_timeout_messages(self):
        self.assertEqual("30 seconds", describe_wait(30))
        self.assertEqual(
            "the run time remaining less 3600s",
            describe_wait(RunTimeReserve(3600)),
        )
