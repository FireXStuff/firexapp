import time
import types
import unittest
from contextlib import contextmanager
from typing import ClassVar
from unittest import mock

from firexkit.argument_conversion import ConverterRegister
from firexkit.chain import SignatureX, returns
from firexkit.firex_celery import FireXCelery
from firexkit.task import (
    REPLACEMENT_TASK_NAME_POSTFIX,
    FireXTask,
    IllegalTaskNameException,
    convert_to_serializable,
)
from firexkit.testing import ut_celery_app


class TaskTests(unittest.TestCase):
    def test_signature_type(self):
        test_app = ut_celery_app()
        self.assertIsInstance(test_app, FireXCelery)

        @test_app.task(base=FireXTask)
        def task(arg=None):
            return arg

        for signature in (task.s(), task.si(), task.signature(), task.s().clone()):
            with self.subTest(signature=signature):
                self.assertIsInstance(signature, SignatureX)
                self.assertIs(signature.app, test_app)

    def test_instantiation(self):
        from celery.utils.threads import LocalStack

        with self.subTest("Name can't end with _orig"):
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = (
                    self.__module__
                    + "."
                    + self.__class__.__name__
                    + "."
                    + f"TestClass{REPLACEMENT_TASK_NAME_POSTFIX}"
                )

            with self.assertRaises(IllegalTaskNameException):
                test_obj = TestTask()

        with self.subTest("Without overrides"):
            # Make sure you can instantiate without the need for the pre and post overrides
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = (
                    self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"
                )

                def run(self):
                    pass

            test_obj = TestTask()
            self.assertIsNotNone(test_obj, "Task object not instantiated")
            self.assertTrue(callable(test_obj.undecorated))

            test_obj.request_stack = LocalStack()  # simulate binding
            test_obj()

        with self.subTest("With overrides"):
            # create a class using the override
            class TestTask(FireXTask):
                ran = False
                pre_ran = False
                post_ran = False
                name = (
                    self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"
                )

                def pre_task_run(self):
                    TestTask.pre_ran = True

                def run(self):
                    TestTask.ran = True

                def _process_result(self, *args, **kwargs):
                    r = super()._process_result(*args, **kwargs)
                    TestTask.post_ran = True
                    return r

            test_obj = TestTask()
            self.assertIsNotNone(test_obj, "Task object not instantiated")
            self.assertTrue(callable(test_obj.undecorated))

            test_obj.request_stack = LocalStack()  # simulate binding
            test_obj()
            self.assertTrue(TestTask.pre_ran, "pre_task_run() was not called")
            self.assertTrue(TestTask.ran, "run() was not called")
            self.assertTrue(TestTask.post_ran, "post_task_run() was not called")

        with self.subTest("Must have Run"):
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = (
                    self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"
                )

            test_obj = TestTask()
            test_obj.request_stack = LocalStack()  # simulate binding
            with self.assertRaises(NotImplementedError):
                test_obj()

    def test_task_argument_conversion(self):
        from celery.utils.threads import LocalStack

        # noinspection PyAbstractClass
        class TestTask(FireXTask):
            name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"
            pre_ran = False
            post_ran = False

            def run(self):
                pass

        @ConverterRegister.register_for_task(TestTask, True)
        def pre(_):
            TestTask.pre_ran = True

        @ConverterRegister.register_for_task(TestTask, False)
        def post(_):
            TestTask.post_ran = True

        test_obj = TestTask()
        test_obj.request_stack = LocalStack()  # simulate binding
        test_obj()
        self.assertTrue(TestTask.pre_ran, "pre_task_run() was not called")
        self.assertTrue(TestTask.post_ran, "post_task_run() was not called")

    def test_undecorated(self):
        test_app = ut_celery_app()

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def a(myself, something):
            return something

        @test_app.task(base=FireXTask)
        def b(something):
            return something

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        @returns("something")
        def c(myself, something):
            return something

        @test_app.task(base=FireXTask)
        @returns("something")
        def d(something):
            return something

        for micro in [a, b, c, d]:
            with self.subTest(micro):
                the_sent_something = "something"
                result = micro.undecorated(the_sent_something)
                self.assertEqual(the_sent_something, result)

    def test_properties(self):
        the_test = self
        test_app = ut_celery_app()

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def a(myself, arg1):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def b(myself, arg1=None):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def c(myself, arg1, arg2=None):
            pass

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def d(myself, arg1, arg2=None, **some_optional_kwargs):
            pass

        with self.subTest("One required argument"):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, (value,))
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, ["arg1"])
                the_test.assertDictEqual(self.bound_args, {"arg1": value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {"arg1": value})
                the_test.assertDictEqual(self.context.bog.return_args, {"arg1": value})
                the_test.assertDictEqual(self.abog.copy(), {"arg1": value})

            a._process_result = types.MethodType(post_task_run, a)
            a(value)

        with self.subTest("One required argument with keyword"):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, ())
                the_test.assertDictEqual(self.kwargs, {"arg1": value})
                the_test.assertListEqual(self.required_args, ["arg1"])
                the_test.assertDictEqual(self.bound_args, {"arg1": value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {"arg1": value})
                the_test.assertDictEqual(
                    self.context.bog.return_args.copy(), {"arg1": value}
                )
                the_test.assertDictEqual(self.abog.copy(), {"arg1": value})

            a._process_result = types.MethodType(post_task_run, a)
            a(arg1=value)

        with self.subTest("One optional argument"):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, (value,))
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, [])
                the_test.assertDictEqual(self.bound_args, {"arg1": value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {"arg1": value})
                the_test.assertDictEqual(
                    self.context.bog.return_args.copy(), {"arg1": value}
                )
                the_test.assertDictEqual(self.abog.copy(), {"arg1": value})

            b._process_result = types.MethodType(post_task_run, b)
            b(value)

        with self.subTest("One optional argument with no value"):
            value = None

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, ())
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, [])
                the_test.assertDictEqual(self.bound_args, {})
                the_test.assertDictEqual(self.default_bound_args, {"arg1": value})
                the_test.assertDictEqual(self.all_args.copy(), {"arg1": value})
                the_test.assertDictEqual(self.context.bog.return_args.copy(), {})
                the_test.assertDictEqual(self.abog.copy(), {"arg1": value})
                self.abog["d"] = 1

            b._process_result = types.MethodType(post_task_run, b)
            b()

        with self.subTest("One optional argument with keyword"):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, ())
                the_test.assertDictEqual(self.kwargs, {"arg1": value})
                the_test.assertListEqual(self.required_args, [])
                the_test.assertDictEqual(self.bound_args, {"arg1": value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {"arg1": value})
                the_test.assertDictEqual(
                    self.context.bog.return_args.copy(), {"arg1": value}
                )
                the_test.assertDictEqual(self.abog.copy(), {"arg1": value})

            b._process_result = types.MethodType(post_task_run, b)
            b(arg1=value)

        with self.subTest("One required and one optional argument "):
            value1 = 1
            value2 = 2

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, (value1, value2))
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, ["arg1"])
                the_test.assertDictEqual(
                    self.bound_args, {"arg1": value1, "arg2": value2}
                )
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(
                    self.all_args.copy(), {"arg1": value1, "arg2": value2}
                )
                the_test.assertDictEqual(
                    self.context.bog.return_args.copy(),
                    {"arg1": value1, "arg2": value2},
                )
                the_test.assertDictEqual(
                    self.abog.copy(), {"arg1": value1, "arg2": value2}
                )

            c._process_result = types.MethodType(post_task_run, c)
            c(value1, value2)

        with self.subTest("One required and one optional argument with keyword"):
            value1 = 1
            value2 = 2

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, ())
                the_test.assertDictEqual(self.kwargs, {"arg1": value1, "arg2": value2})
                the_test.assertListEqual(self.required_args, ["arg1"])
                the_test.assertDictEqual(
                    self.bound_args, {"arg1": value1, "arg2": value2}
                )
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(
                    self.all_args.copy(), {"arg1": value1, "arg2": value2}
                )
                the_test.assertDictEqual(
                    self.context.bog.return_args.copy(),
                    {"arg1": value1, "arg2": value2},
                )
                the_test.assertDictEqual(
                    self.abog.copy(), {"arg1": value1, "arg2": value2}
                )

            c._process_result = types.MethodType(post_task_run, c)
            c(arg2=value2, arg1=value1)

        with self.subTest("One required, one optional provided"):
            value1 = 1
            value2 = None

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, (value1,))
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, ["arg1"])
                the_test.assertDictEqual(self.bound_args, {"arg1": value1})
                the_test.assertDictEqual(self.default_bound_args, {"arg2": value2})
                the_test.assertDictEqual(
                    self.all_args.copy(), {"arg1": value1, "arg2": value2}
                )
                the_test.assertDictEqual(
                    self.context.bog.return_args.copy(), {"arg1": value1}
                )
                the_test.assertDictEqual(
                    self.abog.copy(), {"arg1": value1, "arg2": value2}
                )

            c._process_result = types.MethodType(post_task_run, c)
            c(value1)

        with self.subTest("One required and one optional argument with other optional"):
            value1 = 1
            value2 = 2

            def post_task_run(self, results, extra_events=None):
                the_test.assertEqual(self.args, (value1,))
                the_test.assertDictEqual(self.kwargs, {"arg2": value2, "arg3": 3})
                the_test.assertListEqual(self.required_args, ["arg1"])
                the_test.assertDictEqual(self.optional_args, {"arg2": None})
                the_test.assertDictEqual(
                    self.bound_args,
                    {
                        "arg1": value1,
                        "arg2": value2,
                        "some_optional_kwargs": {"arg3": 3},
                    },
                )
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertEqual(self.all_args, self.bound_args)
                the_test.assertEqual(
                    self.context.bog.return_args,
                    {"arg1": value1, "arg2": value2, "arg3": 3},
                )
                the_test.assertDictEqual(
                    self.abog, {"arg1": value1, "arg2": value2, "arg3": 3}
                )

            d._process_result = types.MethodType(post_task_run, d)
            d(value1, arg2=value2, arg3=3)


class EnqueueManyTests(unittest.TestCase):
    @staticmethod
    def _enqueue(chains):
        task = mock.Mock(spec=FireXTask)
        results = [mock.Mock(), mock.Mock()]
        task.enqueue_child.side_effect = results
        return FireXTask.enqueue_many(task, chains), results

    def test_sequence_results_use_integer_keys(self):
        chains = [mock.Mock(spec=SignatureX), mock.Mock(spec=SignatureX)]

        many_results, results = self._enqueue(chains)

        self.assertEqual(many_results.as_dict(), dict(enumerate(results)))

    def test_mapping_results_preserve_input_keys(self):
        chains = {
            "first": mock.Mock(spec=SignatureX),
            "second": mock.Mock(spec=SignatureX),
        }

        many_results, results = self._enqueue(chains)

        self.assertEqual(many_results.as_dict(), dict(zip(chains, results)))


class TaskCachingTests(unittest.TestCase):
    def test_use_cache(self):
        test_app = ut_celery_app()

        with self.subTest("use_cache is not defined"):

            @test_app.task(base=FireXTask)
            def a():
                pass

            self.assertFalse(a.is_cache_enabled())

        with self.subTest("use_cache is set to True"):

            @test_app.task(base=FireXTask, use_cache=True)
            def b():
                pass

            self.assertTrue(b.is_cache_enabled())

        with self.subTest("use_cache is set to False"):

            @test_app.task(base=FireXTask, use_cache=False)
            def c():
                pass

            self.assertFalse(c.is_cache_enabled())


class ConvertToSerializableTests(unittest.TestCase):
    d: ClassVar[dict] = {"a": 1, "b": ["2", "3"], "c": "4", "d": {"d1": 5, "d2": "6"}}

    def test_dicts_returned_as_is(self):
        self.assertDictEqual(convert_to_serializable(self.d), self.d)

    def test_fallback_to_repr(self):
        repr_str = "Should serialize to this"

        class someClass:
            def __repr__(_self):
                return repr_str

        self.assertEqual(convert_to_serializable(someClass()), repr_str)

    def test_firex_serializable(self):
        class someClass:
            def firex_serializable(_self):
                return self.d

            def __repr__(_self):
                return "Shouldn't serialize to this"

        self.assertDictEqual(convert_to_serializable(someClass()), self.d)

    def test_some_parts_are_jsonifable(self):
        class UnJsonfiableClass:
            pass

        unjsonfiable = UnJsonfiableClass()

        with self.subTest("Outer data structure is a dict:"):
            d2 = dict(**self.d, some_unjsonfiable_object=unjsonfiable)
            expected_result = dict(
                **self.d, some_unjsonfiable_object=repr(unjsonfiable)
            )
            self.assertDictEqual(convert_to_serializable(d2), expected_result)

        with self.subTest("Outer data structure is an iterable:"):
            d2 = [self.d, unjsonfiable]
            expected_result = [self.d, repr(unjsonfiable)]
            self.assertListEqual(convert_to_serializable(d2), expected_result)

    def test_max_recusrive_depth(self):
        class someClass:
            def firex_serializable(_self):
                return self.d

        serializable_obj = someClass()
        level3 = {"level3": serializable_obj}
        level2 = {"level2": level3}
        level1 = {"level1": level2}
        d2 = [self.d, level1]

        with self.subTest("max_recrusive_depth not reached"):
            expected_result = [self.d, {"level1": {"level2": {"level3": self.d}}}]
            self.assertListEqual(
                convert_to_serializable(d2, max_recursive_depth=10), expected_result
            )

        with self.subTest("max_recrusive_depth reached"):
            expected_result = [self.d, {"level1": {"level2": repr(level3)}}]
            self.assertListEqual(
                convert_to_serializable(d2, max_recursive_depth=3), expected_result
            )


class RunTimeLimitTests(unittest.TestCase):
    """
    A task deciding the run needs to be longer than it was submitted for.
    """

    def create_task(self, task_id="me", own_soft_time_limit=100, elapsed_in_task=0):
        test_app = ut_celery_app()

        @test_app.task(base=FireXTask, bind=True)
        def LongTask(self):
            pass

        task = LongTask
        task.increase_calls = []
        task.app.increase_run_soft_time_limit = mock.Mock(
            side_effect=task.increase_calls.append,
        )
        task.app.set_task_soft_time_limit = mock.Mock()
        task.request.id = task_id
        task.request.hostname = "my_worker"
        task.request.timelimit = [None, own_soft_time_limit]
        task.duration = lambda: elapsed_in_task
        return task

    @contextmanager
    def run_time_remaining(self, remaining, elapsed=3600):
        """Pretends the run has `remaining` seconds left, `elapsed` seconds in."""
        with (
            mock.patch(
                "firexkit.run_time.get_run_deadline",
                side_effect=lambda app=None: time.time() + remaining,
            ),
            mock.patch(
                "firexkit.task.get_run_start_time",
                side_effect=lambda app=None: time.time() - elapsed,
            ),
        ):
            yield

    def test_reports_the_run_time_left_after_a_reserve(self):
        task = self.create_task()
        with self.run_time_remaining(10 * 60 * 60):
            self.assertAlmostEqual(
                9 * 60 * 60,
                task.get_run_time_remaining(reserve=60 * 60),
                delta=2,
            )

    def test_a_sufficient_budget_is_left_alone(self):
        task = self.create_task(own_soft_time_limit=None)
        with self.run_time_remaining(10 * 60 * 60):
            task.ensure_run_time_remaining(60 * 60)

        task.app.increase_run_soft_time_limit.assert_not_called()

    def test_raises_the_budget_to_cover_the_need_and_the_reserve(self):
        task = self.create_task()
        with self.run_time_remaining(60 * 60, elapsed=3600):
            task.ensure_run_time_remaining(9 * 60 * 60, reserve=60 * 60)

        (required_total,) = task.increase_calls
        # 1h elapsed + 9h needed + 1h reserve.
        self.assertAlmostEqual(11 * 60 * 60, required_total, delta=2)

    def test_extends_its_own_limit_even_when_the_budget_is_already_enough(self):
        # The run has 10h left, but this task was given 100s.
        task = self.create_task(own_soft_time_limit=100, elapsed_in_task=10)
        with self.run_time_remaining(10 * 60 * 60):
            task.ensure_run_time_remaining(9 * 60 * 60)

        task.app.increase_run_soft_time_limit.assert_not_called()
        task.app.set_task_soft_time_limit.assert_called_once_with(
            "me",
            10 + 9 * 60 * 60,
            destination=["my_worker"],
            increase_only=True,
        )

    def test_leaves_its_own_limit_alone_when_it_already_covers_the_need(self):
        task = self.create_task(own_soft_time_limit=10 * 60 * 60)
        with self.run_time_remaining(10 * 60 * 60):
            task.ensure_run_time_remaining(60 * 60)

        task.app.set_task_soft_time_limit.assert_not_called()

    def test_a_task_with_no_limit_of_its_own_still_extends_itself(self):
        # A published timelimit of None does not mean unlimited: billiard falls back to
        # the worker pool's default soft_timeout, which FireX seeds from the run budget
        # and a pool child cannot read. So the request goes out and the worker, which
        # can see the real limit, drops it if the task already has more.
        task = self.create_task(own_soft_time_limit=None)
        with self.run_time_remaining(10 * 60 * 60):
            task.ensure_run_time_remaining(9 * 60 * 60)

        task.app.set_task_soft_time_limit.assert_called_once_with(
            "me",
            9 * 60 * 60,
            destination=["my_worker"],
            increase_only=True,
        )

    def test_the_increase_is_confirmed_against_its_own_worker(self):
        # increase_run_soft_time_limit does not collect replies, so nothing about it
        # says this task's own limit has moved yet. Naming the one worker that can
        # answer is both the confirmation and the reason the wait is short: a wait
        # that lasted a fixed timeout would spend the seconds just asked for.
        task = self.create_task(own_soft_time_limit=None, elapsed_in_task=12)
        with self.run_time_remaining(-9, elapsed=12):
            task.ensure_run_time_remaining(10)

        self.assertEqual(1, len(task.increase_calls))
        task.app.set_task_soft_time_limit.assert_called_once_with(
            "me",
            12 + 10,
            destination=["my_worker"],
            increase_only=True,
        )

    def test_a_need_smaller_than_the_resolve_floor_still_raises_the_budget(self):
        # The run is 9s past its deadline, so there is no time for a 10s need. Deciding
        # against get_run_time_remaining() would see DEFAULT_MINIMUM_RUN_TIME_REMAINING
        # instead -- time the run does not have -- and skip the increase entirely.
        task = self.create_task()
        with self.run_time_remaining(-9, elapsed=12):
            task.ensure_run_time_remaining(10)

        (required_total,) = task.increase_calls
        # 12s elapsed + 10s needed.
        self.assertAlmostEqual(22, required_total, delta=2)

    def test_returns_the_run_time_remaining_after_the_increase(self):
        task = self.create_task(own_soft_time_limit=None)
        with self.run_time_remaining(10 * 60 * 60):
            remaining = task.ensure_run_time_remaining(60 * 60, reserve=60 * 60)

        self.assertAlmostEqual(9 * 60 * 60, remaining, delta=2)
