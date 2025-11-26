import unittest
import sys
import types

from firexkit.argument_conversion import ConverterRegister
from firexkit.chain import returns
from firexkit.task import FireXTask, task_prerequisite, convert_to_serializable, IllegalTaskNameException, \
    REPLACEMENT_TASK_NAME_POSTFIX
from firexapp.engine.firex_celery import FireXCelery

class TaskTests(unittest.TestCase):

    def test_instantiation(self):
        from celery.utils.threads import LocalStack

        with self.subTest("Name can't end with _orig"):
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = self.__module__ + "." + self.__class__.__name__ + "." \
                       + f"TestClass{REPLACEMENT_TASK_NAME_POSTFIX}"

            with self.assertRaises(IllegalTaskNameException):
                test_obj = TestTask()

        with self.subTest("Without overrides"):
            # Make sure you can instantiate without the need for the pre and post overrides
            # noinspection PyAbstractClass
            class TestTask(FireXTask):
                name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"

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
                name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"

                def pre_task_run(self):
                    TestTask.pre_ran = True

                def run(self):
                    TestTask.ran = True

                def post_task_run(self, results, extra_events=None):
                    TestTask.post_ran = True

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
                name = self.__module__ + "." + self.__class__.__name__ + "." + "TestClass"

            test_obj = TestTask()
            test_obj.request_stack = LocalStack()  # simulate binding
            with self.assertRaises(NotImplementedError):
                test_obj()

    def test_task_argument_conversion(self):
        from firexkit.argument_conversion import ConverterRegister
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
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        def a(myself, something):
            return something

        @test_app.task(base=FireXTask)
        def b(something):
            return something

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, bind=True)
        @returns('something')
        def c(myself, something):
            return something

        @test_app.task(base=FireXTask)
        @returns('something')
        def d(something):
            return something

        for micro in [a, b, c, d]:
            with self.subTest(micro):
                the_sent_something = "something"
                result = micro.undecorated(the_sent_something)
                self.assertEqual(the_sent_something, result)

    def test_prerequisite(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        def something():
            # Should not reach here
            pass  # pragma: no cover

        @task_prerequisite(something, trigger=lambda _: False)
        @test_app.task(base=FireXTask)
        def needs_a_little_something():
            # Should not reach here
            pass  # pragma: no cover

        self.assertTrue(len(ConverterRegister.list_converters(needs_a_little_something.__name__)) == 1)

        with self.assertRaises(Exception):
            @task_prerequisite(something, trigger=None)
            @test_app.task(base=FireXTask)
            def go_boom():
                # Should not reach here
                pass  # pragma: no cover

    def test_properties(self):
        the_test = self
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

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

        with self.subTest('One required argument'):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [value])
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, ['arg1'])
                the_test.assertDictEqual(self.bound_args, {'arg1': value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value})

            a.post_task_run = types.MethodType(post_task_run, a)
            a(value)

        with self.subTest('One required argument with keyword'):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [])
                the_test.assertDictEqual(self.kwargs, {'arg1': value})
                the_test.assertListEqual(self.required_args, ['arg1'])
                the_test.assertDictEqual(self.bound_args, {'arg1': value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value})

            a.post_task_run = types.MethodType(post_task_run, a)
            a(arg1=value)

        with self.subTest('One optional argument'):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [value])
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, [])
                the_test.assertDictEqual(self.bound_args, {'arg1': value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value})

            b.post_task_run = types.MethodType(post_task_run, b)
            b(value)

        with self.subTest('One optional argument with no value'):
            value = None

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [])
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, [])
                the_test.assertDictEqual(self.bound_args, {})
                the_test.assertDictEqual(self.default_bound_args, {'arg1': value})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value})
                the_test.assertDictEqual(self.bag.copy(), {})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value})
                self.abog['d'] = 1

            b.post_task_run = types.MethodType(post_task_run, b)
            b()

        with self.subTest('One optional argument with keyword'):
            value = 1

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [])
                the_test.assertDictEqual(self.kwargs, {'arg1': value})
                the_test.assertListEqual(self.required_args, [])
                the_test.assertDictEqual(self.bound_args, {'arg1': value})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value})

            b.post_task_run = types.MethodType(post_task_run, b)
            b(arg1=value)

        with self.subTest('One required and one optional argument '):
            value1 = 1
            value2 = 2

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [value1, value2])
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, ['arg1'])
                the_test.assertDictEqual(self.bound_args, {'arg1': value1,
                                                           'arg2': value2})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value1,
                                                                'arg2': value2})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value1,
                                                           'arg2': value2})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value1,
                                                            'arg2': value2})

            c.post_task_run = types.MethodType(post_task_run, c)
            c(value1, value2)

        with self.subTest('One required and one optional argument with keyword'):
            value1 = 1
            value2 = 2

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [])
                the_test.assertDictEqual(self.kwargs, {'arg1': value1,
                                                       'arg2': value2})
                the_test.assertListEqual(self.required_args, ['arg1'])
                the_test.assertDictEqual(self.bound_args, {'arg1': value1,
                                                           'arg2': value2})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value1,
                                                                'arg2': value2})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value1,
                                                           'arg2': value2})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value1,
                                                            'arg2': value2})

            c.post_task_run = types.MethodType(post_task_run, c)
            c(arg2=value2, arg1=value1)

        with self.subTest('One required, one optional provided'):
            value1 = 1
            value2 = None

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [value1])
                the_test.assertDictEqual(self.kwargs, {})
                the_test.assertListEqual(self.required_args, ['arg1'])
                the_test.assertDictEqual(self.bound_args, {'arg1': value1})
                the_test.assertDictEqual(self.default_bound_args, {'arg2': value2})
                the_test.assertDictEqual(self.all_args.copy(), {'arg1': value1,
                                                                'arg2': value2})
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value1})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value1,
                                                            'arg2': value2})

            c.post_task_run = types.MethodType(post_task_run, c)
            c(value1)

        with self.subTest('One required and one optional argument with other optional'):
            value1 = 1
            value2 = 2

            def post_task_run(self, results, extra_events=None):
                the_test.assertListEqual(self.args, [value1])
                the_test.assertDictEqual(self.kwargs, {'arg2': value2,
                                                       'arg3': 3})
                the_test.assertListEqual(self.required_args, ['arg1'])
                the_test.assertDictEqual(self.optional_args, {'arg2': None})
                the_test.assertDictEqual(self.bound_args, {'arg1': value1,
                                                           'arg2': value2,
                                                           'some_optional_kwargs': {'arg3': 3}})
                the_test.assertDictEqual(self.default_bound_args, {})
                the_test.assertDictEqual(self.all_args.copy(), self.bound_args)
                the_test.assertDictEqual(self.bag.copy(), {'arg1': value1,
                                                           'arg2': value2,
                                                           'arg3': 3})
                the_test.assertDictEqual(self.abog.copy(), {'arg1': value1,
                                                            'arg2': value2,
                                                            'arg3': 3})

            d.post_task_run = types.MethodType(post_task_run, d)
            d(value1, arg2=value2, arg3=3)

    def test_sig_bind(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

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
        def d(myself, arg1, arg2=None, **kwargs):
            pass

        with self.subTest():
            value = 1
            r = a.map_args(value)
            expected_result = {'arg1': value}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            value = {'k1': 'v1'}
            r = a.map_args(value)
            expected_result = {'arg1': value}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            with self.assertRaises(TypeError):
                a.map_args()

        with self.subTest():
            input_args = {'arg1': 1}
            r = a.map_args(**input_args)
            self.assertDictEqual(r, input_args)

        with self.subTest():
            expected_result = {'arg1': 1}
            other = {'arg2': 2}
            input_args = {**expected_result, **other}
            r = a.map_args(**input_args)
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            expected_result = {'arg1': None}
            r = b.map_args()
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            input_relevant = {'arg1': 1}
            other = {'arg3': 3}
            input_args = {**input_relevant, **other}
            r = c.map_args(**input_args)
            default = {'arg2': None}
            expected_result = {**input_relevant, **default}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            input_args = {'arg1': 1, 'arg2': 2}
            r = c.map_args(**input_args)
            self.assertDictEqual(r, input_args)

        with self.subTest():
            value = 1
            input_args = {'arg2': 2}
            r = c.map_args(value, **input_args)
            expected_result = {'arg1': value, **input_args}
            self.assertDictEqual(r, expected_result)

        with self.subTest():
            input_args = {'arg2': 2}
            with self.assertRaises(TypeError):
                c.map_args(**input_args)

        with self.subTest():
            value = 1
            input_args = {'arg2': 2}
            kwargs = {'some_key': 'some_value'}
            r = d.map_args(value, **input_args, **kwargs)
            expected_result = {'arg1': value, **input_args, **{'kwargs': {**kwargs}}}
            self.assertDictEqual(r, expected_result)


class TaskCachingTests(unittest.TestCase):

    def test_use_cache(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        with self.subTest('use_cache is not defined'):
            @test_app.task(base=FireXTask)
            def a():
                pass

            self.assertFalse(a.is_cache_enabled())

        with self.subTest('use_cache is set to True'):
            @test_app.task(base=FireXTask, use_cache=True)
            def b():
                pass

            self.assertTrue(b.is_cache_enabled())

        with self.subTest('use_cache is set to False'):
            @test_app.task(base=FireXTask, use_cache=False)
            def c():
                pass

            self.assertFalse(c.is_cache_enabled())


class ConvertToSerializableTests(unittest.TestCase):
    d = dict(a=1, b=['2', '3'], c='4', d=dict(d1=5, d2='6'))

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

        with self.subTest('Outer data structure is a dict:'):
            d2 = dict(**self.d, some_unjsonfiable_object=unjsonfiable)
            expected_result = dict(**self.d, some_unjsonfiable_object=repr(unjsonfiable))
            self.assertDictEqual(convert_to_serializable(d2), expected_result)

        with self.subTest('Outer data structure is an iterable:'):
            d2 = [self.d, unjsonfiable]
            expected_result = [self.d, repr(unjsonfiable)]
            self.assertListEqual(convert_to_serializable(d2), expected_result)

    def test_max_recusrive_depth(self):
        class someClass:
            def firex_serializable(_self):
                return self.d

        serializable_obj = someClass()
        level3 = dict(level3=serializable_obj)
        level2 = dict(level2=level3)
        level1 = dict(level1=level2)
        d2 = [self.d, level1]

        with self.subTest('max_recrusive_depth not reached'):
            expected_result = [self.d, dict(level1=dict(level2=dict(level3=self.d)))]
            self.assertListEqual(convert_to_serializable(d2, max_recursive_depth=10), expected_result)

        with self.subTest('max_recrusive_depth reached'):
            expected_result = [self.d, dict(level1=dict(level2=repr(level3)))]
            self.assertListEqual(convert_to_serializable(d2, max_recursive_depth=3), expected_result)
