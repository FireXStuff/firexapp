import datetime
import unittest
from time import monotonic
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytz

from firexapp.submit.uid import firex_id_str
from firexkit.firex_celery import (
    _FX_HARD,
    _FX_SOFT,
    FX_TIME_RESERVE_HEADER,
    FireXAsynPool,
    FireXCelery,
    FireXTaskPool,
    _fx_increase_run_soft_time_limit,
    _request_run_time_reserve,
)
from firexkit.run_time import RunTimeReserve

_RUN_START = datetime.datetime(2026, 9, 14, 12, 0, 0, tzinfo=pytz.utc)
_RUN_START_EPOCH = _RUN_START.timestamp()
_TEN_HOURS = 10 * 60 * 60


def _create_app(run_soft_time_limit=_TEN_HOURS):
    """A stand-in for FireXCelery holding just what the time limit code reads."""
    return SimpleNamespace(
        conf=SimpleNamespace(
            fx_env=SimpleNamespace(firex_id=firex_id_str('someuser', _RUN_START, 1234)),
            task_soft_time_limit=72 * 60,
            run_soft_time_limit=run_soft_time_limit,
        ),
        tasks={},
        get_run_soft_time_limit=lambda: run_soft_time_limit,
        _cache_run_soft_time_limit=Mock(),
    )


def _at_run_elapsed(seconds):
    return patch('time.time', return_value=_RUN_START_EPOCH + seconds)


class _FakeTimerEntry:
    """Stand-in for kombu.asynchronous.timer.Entry."""

    def __init__(self, delay, fun, args):
        self.delay = delay
        self.fun = fun
        self.args = args
        self.canceled = False

    def cancel(self):
        self.canceled = True

    def __call__(self):
        self.fun(*self.args)


class _FakeHub:
    def __init__(self):
        self.entries = []

    def call_later(self, delay, fun, *args):
        entry = _FakeTimerEntry(delay, fun, args)
        self.entries.append(entry)
        return entry


def _create_pool(hub=None):
    # Bypass __init__ to avoid forking a real pool.
    pool = FireXAsynPool.__new__(FireXAsynPool)
    pool._fx_trefs = {}
    pool._fx_hub = hub
    pool._cache = {}
    pool.on_soft_timeout = Mock()
    pool.on_hard_timeout = Mock()
    return pool


def _create_result(job=1, task_id='task-1', soft=100, hard=None, accepted=True):
    return SimpleNamespace(
        _job=job,
        correlation_id=task_id,
        _soft_timeout=soft,
        _timeout=hard,
        _time_accepted=monotonic() if accepted else None,
    )


class FireXAsynPoolTimerTests(unittest.TestCase):

    def test_arms_soft_and_hard_independently(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)

        pool._fx_on_timeout_set(result, 100, 200)

        trefs = pool._fx_trefs[result._job]
        self.assertEqual({_FX_SOFT, _FX_HARD}, set(trefs))
        self.assertAlmostEqual(100, trefs[_FX_SOFT].delay, delta=1)
        self.assertAlmostEqual(200, trefs[_FX_HARD].delay, delta=1)

    def test_arms_deadlines_relative_to_accept_time(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        # Pretend the job was accepted 30s ago.
        result._time_accepted -= 30

        pool._fx_on_timeout_set(result, 100, 200)

        trefs = pool._fx_trefs[result._job]
        self.assertAlmostEqual(70, trefs[_FX_SOFT].delay, delta=1)
        self.assertAlmostEqual(170, trefs[_FX_HARD].delay, delta=1)

    def test_changing_soft_leaves_hard_tref_untouched(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=1000)
        pool._fx_on_timeout_set(result, 100, 1000)
        original_soft = pool._fx_trefs[result._job][_FX_SOFT]
        original_hard = pool._fx_trefs[result._job][_FX_HARD]

        applied = pool.set_job_soft_time_limit(result, 500)

        self.assertEqual(500, applied)
        self.assertEqual(500, result._soft_timeout)
        self.assertTrue(original_soft.canceled)
        self.assertFalse(original_hard.canceled)
        trefs = pool._fx_trefs[result._job]
        self.assertIs(original_hard, trefs[_FX_HARD])
        self.assertIsNot(original_soft, trefs[_FX_SOFT])
        self.assertAlmostEqual(500, trefs[_FX_SOFT].delay, delta=1)

    def test_clamps_soft_to_hard_time_limit(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._fx_on_timeout_set(result, 100, 200)

        applied = pool.set_job_soft_time_limit(result, 1000)

        self.assertEqual(200, applied)
        self.assertEqual(200, result._soft_timeout)
        self.assertAlmostEqual(
            200, pool._fx_trefs[result._job][_FX_SOFT].delay, delta=1,
        )

    def test_does_not_clamp_without_hard_time_limit(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(soft=100, hard=None)

        self.assertEqual(1000, pool.set_job_soft_time_limit(result, 1000))

    def test_lowering_below_elapsed_fires_immediately(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=1000)
        result._time_accepted -= 60
        pool._fx_on_timeout_set(result, 1000, None)

        pool.set_job_soft_time_limit(result, 10)

        self.assertEqual(0, pool._fx_trefs[result._job][_FX_SOFT].delay)

    def test_none_soft_time_limit_cancels_soft_tref(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._fx_on_timeout_set(result, 100, 200)
        original_soft = pool._fx_trefs[result._job][_FX_SOFT]

        self.assertIsNone(pool.set_job_soft_time_limit(result, None))

        self.assertTrue(original_soft.canceled)
        self.assertNotIn(_FX_SOFT, pool._fx_trefs[result._job])
        self.assertIn(_FX_HARD, pool._fx_trefs[result._job])

    def test_unaccepted_job_records_limit_without_arming(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, accepted=False)

        self.assertEqual(500, pool.set_job_soft_time_limit(result, 500))

        self.assertEqual(500, result._soft_timeout)
        self.assertEqual([], hub.entries)
        self.assertEqual({}, pool._fx_trefs)

    def test_soft_expiry_signals_job_and_leaves_hard_armed(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._cache[result._job] = result
        pool._fx_on_timeout_set(result, 100, 200)
        hard_tref = pool._fx_trefs[result._job][_FX_HARD]

        pool._fx_trefs[result._job][_FX_SOFT]()

        pool.on_soft_timeout.assert_called_once_with(result)
        pool.on_hard_timeout.assert_not_called()
        self.assertFalse(hard_tref.canceled)
        self.assertIs(hard_tref, pool._fx_trefs[result._job][_FX_HARD])

    def test_hard_expiry_signals_job(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._cache[result._job] = result
        pool._fx_on_timeout_set(result, 100, 200)

        pool._fx_trefs[result._job][_FX_HARD]()

        pool.on_hard_timeout.assert_called_once_with(result)

    def test_expiry_of_completed_job_discards_trefs(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._fx_on_timeout_set(result, 100, 200)

        # Job isn't in _cache, i.e. it already completed.
        pool._fx_trefs[result._job][_FX_SOFT]()

        pool.on_soft_timeout.assert_not_called()
        self.assertNotIn(result._job, pool._fx_trefs)

    def test_timeout_cancel_discards_both_trefs(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._fx_on_timeout_set(result, 100, 200)
        trefs = dict(pool._fx_trefs[result._job])

        pool._fx_on_timeout_cancel(result)

        self.assertEqual({}, pool._fx_trefs)
        self.assertTrue(all(tref.canceled for tref in trefs.values()))

    def test_re_registering_with_event_loop_keeps_in_flight_trefs(self):
        hub = _FakeHub()
        pool = _create_pool(hub)
        result = _create_result(soft=100, hard=200)
        pool._fx_on_timeout_set(result, 100, 200)

        # e.g. broker reconnect while jobs are in flight.
        pool._create_timelimit_handlers(hub)

        self.assertIn(result._job, pool._fx_trefs)
        # Bound methods compare equal, but are never identical.
        self.assertEqual(pool._fx_on_timeout_set, pool.on_timeout_set)
        self.assertEqual(pool._fx_on_timeout_cancel, pool.on_timeout_cancel)
        self.assertEqual(pool._fx_discard_trefs, pool._discard_tref)


class FireXTaskPoolTests(unittest.TestCase):

    @staticmethod
    def create_task_pool(pool):
        task_pool = FireXTaskPool.__new__(FireXTaskPool)
        task_pool._pool = pool
        return task_pool

    def test_finds_running_job_by_task_id(self):
        pool = _create_pool(_FakeHub())
        wanted = _create_result(job=2, task_id='wanted')
        pool._cache = {1: _create_result(job=1, task_id='other'), 2: wanted}

        self.assertIs(wanted, self.create_task_pool(pool)._find_job_result('wanted'))

    def test_unknown_task_id_applies_nothing(self):
        pool = _create_pool(_FakeHub())
        pool._cache = {1: _create_result(job=1, task_id='other')}
        task_pool = self.create_task_pool(pool)

        self.assertIsNone(task_pool._find_job_result('missing'))
        self.assertIsNone(task_pool.set_task_soft_time_limit('missing', 500))

    def test_sets_soft_time_limit_of_running_task(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(job=1, task_id='running', soft=100, hard=1000)
        pool._cache = {1: result}
        pool._fx_on_timeout_set(result, 100, 1000)

        applied = self.create_task_pool(pool).set_task_soft_time_limit('running', 500)

        self.assertEqual(500, applied)
        self.assertEqual(500, result._soft_timeout)


class IncreaseOnlyTests(unittest.TestCase):

    def test_refuses_to_lower_an_existing_limit(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(soft=1000)
        pool._fx_on_timeout_set(result, 1000, None)
        original_soft = pool._fx_trefs[result._job][_FX_SOFT]

        applied = pool.set_job_soft_time_limit(result, 100, increase_only=True)

        self.assertEqual(1000, applied)
        self.assertEqual(1000, result._soft_timeout)
        self.assertFalse(original_soft.canceled)

    def test_applies_a_larger_limit(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(soft=1000)
        pool._fx_on_timeout_set(result, 1000, None)

        self.assertEqual(
            5000, pool.set_job_soft_time_limit(result, 5000, increase_only=True),
        )

    def test_leaves_an_unlimited_job_unlimited(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(soft=None)

        self.assertIsNone(
            pool.set_job_soft_time_limit(result, 5000, increase_only=True),
        )
        self.assertIsNone(result._soft_timeout)

    def test_becoming_unlimited_is_an_increase(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(soft=1000)
        pool._fx_on_timeout_set(result, 1000, None)

        self.assertIsNone(
            pool.set_job_soft_time_limit(result, None, increase_only=True),
        )


class RunRelativeTaskPoolTests(unittest.TestCase):

    def create_task_pool(self, pool, app=None, soft_timeout=100):
        task_pool = FireXTaskPool.__new__(FireXTaskPool)
        task_pool._pool = pool
        task_pool.app = app if app is not None else _create_app()
        task_pool.options = {'soft_timeout': soft_timeout}
        pool.soft_timeout = soft_timeout
        return task_pool

    def test_reserve_becomes_a_limit_measured_from_task_start(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(job=1, task_id='running', soft=100)
        # The task has been running for 10 minutes.
        result._time_accepted -= 600
        pool._cache = {1: result}
        pool._fx_on_timeout_set(result, 100, None)
        task_pool = self.create_task_pool(pool)

        # 1h into a 10h run, keeping an hour back: 8h of run time left for it, and a
        # job's soft_time_limit is measured from when the job started.
        with _at_run_elapsed(60 * 60):
            applied = task_pool.set_task_soft_time_limit(
                'running', RunTimeReserve(60 * 60),
            )

        self.assertAlmostEqual(600 + 8 * 60 * 60, applied, delta=1)

    def test_a_running_job_is_aimed_at_the_real_deadline_not_the_floor(self):
        # The case ensure_run_time_remaining creates: a task raises the budget so that
        # exactly `need` seconds remain, then has to be killed when they run out. Going
        # through resolve() instead would hand it DEFAULT_MINIMUM_RUN_TIME_REMAINING and
        # let it outlive the budget recorded in run.json.
        pool = _create_pool(_FakeHub())
        result = _create_result(job=1, task_id='running', soft=3)
        result._time_accepted -= 1
        pool._cache = {1: result}
        pool._fx_on_timeout_set(result, 3, None)
        task_pool = self.create_task_pool(pool, soft_timeout=3)

        # A 23s budget, 13s in: 10s of run left, well under the 60s floor.
        with _at_run_elapsed(13), patch.object(
            task_pool.app, 'get_run_soft_time_limit', return_value=23,
        ):
            applied = task_pool.set_task_soft_time_limit(
                'running', RunTimeReserve(0), increase_only=True,
            )

        self.assertAlmostEqual(1 + 10, applied, delta=1)

    def test_a_running_job_out_of_run_time_gets_a_positive_limit(self):
        # Zero would read as "no limit at all" to billiard.
        pool = _create_pool(_FakeHub())
        result = _create_result(job=1, task_id='running', soft=1000)
        pool._cache = {1: result}
        pool._fx_on_timeout_set(result, 1000, None)
        task_pool = self.create_task_pool(pool)

        with _at_run_elapsed(13), patch.object(
            task_pool.app, 'get_run_soft_time_limit', return_value=13,
        ):
            applied = task_pool.set_task_soft_time_limit('running', RunTimeReserve(0))

        self.assertGreater(applied, 0)
        self.assertLess(applied, 1)

    def test_an_unstarted_job_resolves_the_reserve_directly(self):
        pool = _create_pool(_FakeHub())
        result = _create_result(job=1, task_id='queued', soft=100, accepted=False)
        pool._cache = {1: result}
        task_pool = self.create_task_pool(pool)

        with _at_run_elapsed(60 * 60):
            applied = task_pool.set_task_soft_time_limit(
                'queued', RunTimeReserve(60 * 60),
            )

        self.assertAlmostEqual(8 * 60 * 60, applied, delta=1)

    def test_worker_default_is_raised_but_never_lowered(self):
        task_pool = self.create_task_pool(_create_pool(_FakeHub()), soft_timeout=100)

        self.assertEqual(5000, task_pool.set_default_soft_time_limit(5000))
        self.assertEqual(5000, task_pool._pool.soft_timeout)
        # Takes effect for subsequently dispatched tasks: billiard re-reads
        # self.soft_timeout on every apply_async.
        self.assertEqual(5000, task_pool.options['soft_timeout'])

        self.assertEqual(5000, task_pool.set_default_soft_time_limit(100))
        self.assertEqual(5000, task_pool._pool.soft_timeout)

    def test_an_unlimited_worker_default_stays_unlimited(self):
        task_pool = self.create_task_pool(_create_pool(_FakeHub()), soft_timeout=None)

        self.assertIsNone(task_pool.set_default_soft_time_limit(5000))
        self.assertIsNone(task_pool._pool.soft_timeout)

    def test_a_late_started_worker_seeds_its_default_from_the_budget(self):
        # e.g. a sandbox worker started after some task raised the budget.
        task_pool = self.create_task_pool(_create_pool(_FakeHub()), soft_timeout=100)

        task_pool._fx_seed_soft_timeout_from_run_budget()

        self.assertEqual(_TEN_HOURS, task_pool.options['soft_timeout'])

    def test_seeding_never_shortens_a_larger_worker_default(self):
        task_pool = self.create_task_pool(
            _create_pool(_FakeHub()), soft_timeout=48 * 60 * 60,
        )

        task_pool._fx_seed_soft_timeout_from_run_budget()

        self.assertEqual(48 * 60 * 60, task_pool.options['soft_timeout'])

    def test_an_unreadable_budget_leaves_the_default_alone(self):
        app = _create_app()
        app.get_run_soft_time_limit = Mock(side_effect=OSError('no broker'))
        task_pool = self.create_task_pool(
            _create_pool(_FakeHub()), app=app, soft_timeout=100,
        )

        task_pool._fx_seed_soft_timeout_from_run_budget()

        self.assertEqual(100, task_pool.options['soft_timeout'])


def _create_request(task_id, reserve=None, declared=None, time_limits=(None, None)):
    return SimpleNamespace(
        id=task_id,
        request_dict=({} if reserve is None else {FX_TIME_RESERVE_HEADER: reserve}),
        task=SimpleNamespace(run_time_limit_reserve=declared),
        time_limits=time_limits,
    )


class RequestRunTimeReserveTests(unittest.TestCase):

    def test_published_reserve_wins(self):
        request = _create_request('t', reserve=60, declared=3600)
        self.assertEqual(60, _request_run_time_reserve(request))

    def test_falls_back_to_what_the_task_type_declares(self):
        request = _create_request('t', declared=3600)
        self.assertEqual(3600, _request_run_time_reserve(request))

    def test_a_task_on_the_worker_default_gets_the_whole_remaining_run(self):
        # Nothing was published for it, so it has been following the run budget through
        # the pool default all along.
        request = _create_request('undeclared')
        self.assertEqual(0, _request_run_time_reserve(request))

    def test_an_explicit_limit_is_left_alone(self):
        request = _create_request('explicit', time_limits=(None, 7200))
        self.assertIsNone(_request_run_time_reserve(request))


class IncreaseRunSoftTimeLimitCommandTests(unittest.TestCase):

    def create_state(self, active=(), reserved=(), soft_timeout=100):
        pool = _create_pool(_FakeHub())
        pool._cache = {}
        task_pool = FireXTaskPool.__new__(FireXTaskPool)
        task_pool._pool = pool
        task_pool.app = _create_app()
        task_pool.options = {'soft_timeout': soft_timeout}
        pool.soft_timeout = soft_timeout

        for i, request in enumerate(active, start=1):
            result = _create_result(job=i, task_id=request.id, soft=100)
            pool._cache[i] = result
            pool._fx_on_timeout_set(result, 100, None)

        state = SimpleNamespace(
            consumer=SimpleNamespace(app=task_pool.app, pool=task_pool),
        )
        return state, task_pool, list(active), list(reserved)

    def run_command(self, state, active, reserved, budget=_TEN_HOURS):
        with (
            patch('celery.worker.state.active_requests', active),
            patch('celery.worker.state.reserved_requests', reserved),
            _at_run_elapsed(60 * 60),
        ):
            return _fx_increase_run_soft_time_limit(
                state,
                run_soft_time_limit=budget,
            )

    def test_extends_every_task_that_was_following_the_run_budget(self):
        run_relative = _create_request('run-relative', reserve=60 * 60)
        # Never declared anything, so it has been running on the pool default -- which
        # is the run budget. Raising the budget has to reach it too, or splitting the
        # budget out of --soft_time_limit would strand it at the original number.
        undeclared = _create_request('undeclared')
        explicit = _create_request('explicit', time_limits=(None, 7200))
        state, task_pool, active, reserved = self.create_state(
            active=[run_relative, undeclared, explicit],
        )

        reply = self.run_command(state, active, reserved)

        extended = reply['ok']
        self.assertEqual({'run-relative', 'undeclared'}, set(extended))
        # 1h into a 10h run: 8h left after the reserve, 9h for the undeclared task.
        self.assertAlmostEqual(8 * 60 * 60, extended['run-relative'], delta=2)
        self.assertAlmostEqual(9 * 60 * 60, extended['undeclared'], delta=2)

    def test_raises_the_worker_default_for_tasks_that_declared_nothing(self):
        state, task_pool, active, reserved = self.create_state()

        self.run_command(state, active, reserved)

        self.assertEqual(_TEN_HOURS, task_pool._pool.soft_timeout)

    def test_rewrites_the_limit_of_a_prefetched_task(self):
        # Prefetched, so its timelimit header was built from the old budget and there
        # is no pool job to re-arm.
        prefetched = _create_request(
            'prefetched', reserve=60 * 60, time_limits=[None, 100],
        )
        state, task_pool, active, reserved = self.create_state(reserved=[prefetched])

        reply = self.run_command(state, active, reserved)

        self.assertAlmostEqual(8 * 60 * 60, prefetched.time_limits[1], delta=2)
        self.assertIn('prefetched', reply['ok'])

    def test_a_prefetched_tasks_hard_time_limit_is_kept(self):
        prefetched = _create_request(
            'prefetched', reserve=60 * 60, time_limits=[12345, 100],
        )
        state, task_pool, active, reserved = self.create_state(reserved=[prefetched])

        self.run_command(state, active, reserved)

        self.assertEqual(12345, prefetched.time_limits[0])

    def test_an_active_task_is_not_also_handled_as_reserved(self):
        # A request stays in reserved_requests while it is active.
        request = _create_request('both', reserve=60 * 60, time_limits=[None, 100])
        state, task_pool, active, reserved = self.create_state(
            active=[request], reserved=[request],
        )

        self.run_command(state, active, reserved)

        self.assertEqual([None, 100], request.time_limits)

    def test_caches_the_new_budget_for_the_pool_children(self):
        state, task_pool, active, reserved = self.create_state()

        self.run_command(state, active, reserved, budget=12345)

        task_pool.app._cache_run_soft_time_limit.assert_called_once_with(12345)

    def test_reports_a_pool_that_cannot_change_limits(self):
        state, _, active, reserved = self.create_state()
        state.consumer.pool = SimpleNamespace()

        self.assertIn('error', self.run_command(state, active, reserved))


class ResolveRunRelativeTimeLimitTests(unittest.TestCase):

    def resolve(self, headers, declared=None):
        app = _create_app()
        if declared is not None:
            app.tasks['a.Task'] = SimpleNamespace(run_time_limit_reserve=declared)
        with _at_run_elapsed(60 * 60):
            # Unbound: FireXCelery can't be instantiated without a broker, and this
            # only reaches self.tasks and the run budget.
            FireXCelery._resolve_run_relative_time_limit(app, 'a.Task', headers)
        return headers

    def test_publishes_the_resolved_limit_and_the_reserve(self):
        headers = self.resolve({'timelimit': [None, RunTimeReserve(60 * 60)]})

        self.assertAlmostEqual(8 * 60 * 60, headers['timelimit'][1], delta=2)
        # The reserve travels too, so a later budget increase can recompute the limit
        # of this task while it is running.
        self.assertEqual(60 * 60, headers[FX_TIME_RESERVE_HEADER])

    def test_uses_what_the_task_type_declares(self):
        headers = self.resolve({'timelimit': [None, None]}, declared=60 * 60)

        self.assertAlmostEqual(8 * 60 * 60, headers['timelimit'][1], delta=2)
        self.assertEqual(60 * 60, headers[FX_TIME_RESERVE_HEADER])

    def test_an_enqueued_reserve_beats_the_task_types(self):
        headers = self.resolve(
            {'timelimit': [None, RunTimeReserve(2 * 60 * 60)]}, declared=60 * 60,
        )

        self.assertAlmostEqual(7 * 60 * 60, headers['timelimit'][1], delta=2)
        self.assertEqual(2 * 60 * 60, headers[FX_TIME_RESERVE_HEADER])

    def test_an_explicit_number_is_left_alone(self):
        headers = self.resolve({'timelimit': [None, 30]}, declared=60 * 60)

        self.assertEqual(30, headers['timelimit'][1])
        self.assertNotIn(FX_TIME_RESERVE_HEADER, headers)

    def test_a_task_declaring_nothing_is_left_alone(self):
        headers = self.resolve({'timelimit': [None, None]})

        self.assertIsNone(headers['timelimit'][1])
        self.assertNotIn(FX_TIME_RESERVE_HEADER, headers)

    def test_tolerates_a_message_without_time_limits(self):
        self.assertEqual({}, self.resolve({}))
