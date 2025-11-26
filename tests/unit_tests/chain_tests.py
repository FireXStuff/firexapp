import unittest
import sys
from collections import namedtuple
from celery.canvas import chain

from firexkit.task import FireXTask, get_attr_unwrapped, DyanmicReturnsNotADict
from firexkit.chain import ReturnsCodingException, returns, verify_chain_arguments, InvalidChainArgsException, \
    InjectArgs
from functools import wraps
from firexapp.engine.firex_celery import FireXCelery

def assertTupleAlmostEqual(t1, t2):
    len_t1 = len(t1)
    len_t2 = len(t2)
    assert len_t1 == len_t2, '%d != %d' % (len_t1, len_t2)
    for k in t1:
        assert k in t2, '%r not in %r' % (k, t2)


class ReturnsTests(unittest.TestCase):

    def test_returns_normal_case(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        @returns("stuff")
        def a_task(the_goods):
            return the_goods

        @test_app.task(base=FireXTask, returns=('stuff',))
        def b_task(the_goods):
            return the_goods

        @test_app.task(base=FireXTask)
        @returns(FireXTask.DYNAMIC_RETURN)
        def c_task(the_goods):
            return {'stuff': the_goods}

        @test_app.task(base=FireXTask, returns=FireXTask.DYNAMIC_RETURN)
        def d_task(the_goods):
            return {'stuff': the_goods}

        @test_app.task(base=FireXTask, returns=FireXTask.DYNAMIC_RETURN)
        def e_task(the_goods):
            return ({'stuff': the_goods},)

        for task in [a_task, b_task, c_task, d_task, e_task]:
            with self.subTest():
                ret = task(the_goods="the_goods")
                self.assertTrue(type(ret) is dict)
                self.assertTrue(len(ret) == 3)
                self.assertTrue("stuff" in ret)
                self.assertTrue("the_goods" in ret)
                self.assertEqual("the_goods", ret["the_goods"])
                self.assertEqual(ret["stuff"], ret["the_goods"])
                assertTupleAlmostEqual(ret[FireXTask.RETURN_KEYS_KEY], ('stuff',))

    def test_dynamic_returns(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask, returns=(FireXTask.DYNAMIC_RETURN, 'stuff2'))
        def a_task(the_goods, the_other_goods):
            return {'stuff': the_goods}, the_other_goods

        @test_app.task(base=FireXTask, returns=('stuff2', FireXTask.DYNAMIC_RETURN))
        def b_task(the_other_goods, the_goods):
            return the_other_goods, {'stuff': the_goods}

        @test_app.task(base=FireXTask, returns=(FireXTask.DYNAMIC_RETURN, FireXTask.DYNAMIC_RETURN))
        def c_task(the_goods, the_other_goods):
            return {'stuff': the_goods}, {'stuff2': the_other_goods}

        for task in [a_task, b_task, c_task]:
            with self.subTest('Testing %s' % task.__name__):
                ret = task(the_goods="the_goods", the_other_goods="the_other_goods")
                self.assertTrue(type(ret) is dict)
                self.assertTrue(len(ret) == 5)
                self.assertTrue("stuff" in ret)
                self.assertTrue("stuff2" in ret)
                self.assertTrue("the_goods" in ret)
                self.assertTrue("the_other_goods" in ret)
                self.assertEqual("the_goods", ret["the_goods"])
                self.assertEqual("the_other_goods", ret["the_other_goods"])
                self.assertEqual(ret["stuff"], ret["the_goods"])
                self.assertEqual(ret["stuff2"], ret["the_other_goods"])
                assertTupleAlmostEqual(ret[FireXTask.RETURN_KEYS_KEY], ('stuff', 'stuff2'))

        @test_app.task(base=FireXTask, returns=(FireXTask.DYNAMIC_RETURN, FireXTask.DYNAMIC_RETURN))
        def d_task(the_goods, the_other_goods):
            return {'stuff': the_goods}, {'stuff': the_other_goods}

        with self.subTest('returns are stumping each other'):
            ret = d_task(the_goods="the_goods", the_other_goods="the_other_goods")
            self.assertTrue(type(ret) is dict)
            self.assertTrue(len(ret) == 4)
            self.assertTrue("stuff" in ret)
            self.assertTrue("the_goods" in ret)
            self.assertTrue("the_other_goods" in ret)
            self.assertEqual("the_goods", ret["the_goods"])
            self.assertEqual("the_other_goods", ret["the_other_goods"])
            self.assertIn(ret["stuff"], [ret["the_other_goods"], ret["the_goods"]])
            assertTupleAlmostEqual(ret[FireXTask.RETURN_KEYS_KEY], ('stuff',))

        @test_app.task(base=FireXTask, returns=FireXTask.DYNAMIC_RETURN)
        def e_task(the_goods):
            return the_goods

        for input_value in [None, '', (set(),), set(), dict(), (dict(),), (tuple(),)]:
            with self.subTest():
                ret = e_task(the_goods=input_value)
                self.assertTrue(type(ret) is dict)
                self.assertDictEqual(ret, {'the_goods': input_value})

        @test_app.task(base=FireXTask, returns=['stuff', FireXTask.DYNAMIC_RETURN])
        def f_task(the_goods, the_other_goods):
            return the_goods, the_other_goods

        for bad_input in [{1,2,3}, (1,2,3), [1,2,3], 'some_string']:
            with self.subTest():
                with self.assertRaises(DyanmicReturnsNotADict):
                    f_task(the_goods='something', the_other_goods=bad_input)

        # validate that order is preserved
        @test_app.task(base=FireXTask, returns=['stuff', FireXTask.DYNAMIC_RETURN, "more_stuff"])
        def g_task():
            return "first", {"stuff": "final", "more_stuff": "first"}, "final2"

        with self.subTest("Precedence"):
            for _ in range(0, 5):  # run multiple times to avoid false positives if dict are used
                ret = g_task()
                self.assertEqual(ret["stuff"], "final", "Dynamic did not override stuff")
                self.assertEqual(ret["more_stuff"], "final2", "explicit did not override dynamic")

    def test_bad_returns_code(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        # duplicate keys
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns("a", "a")
            def a_task():
                # Should not reach here
                pass  # pragma: no cover

        # duplicate keys
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask, returns=("a", "a"))
            def dup_return():
                # Should not reach here
                pass  # pragma: no cover
            # Need to instantiate the object (otherwise its just a Proxy), hence the next line
            dup_return.__name__

        # @returns above @app.task
        with self.assertRaises(ReturnsCodingException):
            @returns("a")
            @test_app.task(base=FireXTask)
            def a_task():
                # Should not reach here
                pass  # pragma: no cover

        # no keys in @returns
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns()
            def a_task():
                # Should not reach here
                pass  # pragma: no cover

        # No actual return
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns("a", "b")
            def a_task():
                return
            a_task()

        # No actual return
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask, returns=('a', 'b'))
            def no_returns():
                return
            no_returns()

        # values returned don't match keys
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask)
            @returns("a", "b")
            def another_task():
                return None, None, None
            another_task()

        # values returned don't match keys
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask, returns=('a', 'b'))
            def and_another_task():
                return None, None, None
            and_another_task()

        # Can't use both @returns and returns=
        with self.assertRaises(ReturnsCodingException):
            @test_app.task(base=FireXTask, returns=('a',))
            @returns('a')
            def double_return():
                return None
            # Need to instantiate the object (otherwise its just a Proxy), hence the next line
            # noinspection PyStatementEffect
            double_return.__name__

    def test_returns_and_bind(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask, bind=True)
        @returns("the_task_name", "stuff")
        def a_task(task_self, the_goods):
            return task_self.name, the_goods

        @test_app.task(base=FireXTask, bind=True, returns=("the_task_name", "stuff"))
        def b_task(task_self, the_goods):
            return task_self.name, the_goods

        for task in [a_task, b_task]:
            with self.subTest():
                ret = task(the_goods="the_goods")
                self.assertTrue(type(ret) is dict)
                self.assertTrue(len(ret) == 4)
                self.assertTrue("stuff" in ret)
                self.assertTrue("the_task_name" in ret)
                self.assertTrue("the_goods" in ret)
                # noinspection PyTypeChecker
                self.assertEqual("the_goods", ret["the_goods"])
                # noinspection PyTypeChecker
                self.assertEqual(ret["stuff"], ret["the_goods"])
                assertTupleAlmostEqual(ret[FireXTask.RETURN_KEYS_KEY], ("the_task_name", "stuff"))

    def test_returns_play_nice_with_decorators(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        def passing_through(func):
            @wraps(func)
            def inner(*args, **kwargs):
                return func(*args, **kwargs)
            return inner

        @test_app.task(base=FireXTask)
        @returns("stuff")
        @passing_through
        def a_task(the_goods):
            return the_goods

        ret = a_task(the_goods="the_goods")
        self.assertTrue(type(ret) is dict)
        self.assertTrue(len(ret) == 3)
        self.assertTrue("stuff" in ret)
        assertTupleAlmostEqual(ret[FireXTask.RETURN_KEYS_KEY], ('stuff',))

    def test_returning_named_tuples(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)
        TestingTuple = namedtuple('TestingTuple', ['thing1', 'thing2'])

        @test_app.task(base=FireXTask)
        @returns("named_t")
        def a_task():
            return TestingTuple(thing1=1, thing2="two")

        @test_app.task(base=FireXTask, returns="named_t")
        def b_task():
            return TestingTuple(thing1=1, thing2="two")

        for task in [a_task, b_task]:
            with self.subTest():
                ret = task()
                # noinspection PyTypeChecker
                self.assertTrue(type(ret["named_t"]) is TestingTuple)
                self.assertTrue(type(ret) is dict)
                self.assertTrue(len(ret) == 2)
                self.assertTrue("named_t" in ret)
                assertTupleAlmostEqual(ret[FireXTask.RETURN_KEYS_KEY], ('named_t',))

                # noinspection PyTypeChecker
                self.assertEqual(1, ret["named_t"].thing1)
                # noinspection PyTypeChecker
                self.assertEqual("two", ret["named_t"].thing2)


class ChainVerificationTests(unittest.TestCase):

    def test_detect_missing(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        def task1():
            pass  # pragma: no cover

        @test_app.task(base=FireXTask)
        def task2raise(stuff):
            assert stuff  # pragma: no cover

        @test_app.task(base=FireXTask)
        def task2ok(stuff=None):
            assert stuff  # pragma: no cover

        # fails if it is missing something
        c = chain(task1.s(), task2raise.s())
        with self.assertRaises(InvalidChainArgsException):
            verify_chain_arguments(c)

        # same result a second time
        with self.assertRaises(InvalidChainArgsException):
            verify_chain_arguments(c)

        # pass if it gets what it needs
        c = chain(task1.s(stuff="yes"), task2raise.s())
        verify_chain_arguments(c)

        # default arguments are sufficient
        c = chain(task1.s(), task2ok.s())
        verify_chain_arguments(c)

    def test_indirect(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        @returns("stuff")
        def task1_with_return():
            pass  # pragma: no cover

        @test_app.task(base=FireXTask, returns=set(['stuff']))
        def task1_with_task_return():
            pass  # pragma: no cover

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask)
        def task2needs(thing="@stuff"):
            pass  # pragma: no cover

        for task in [task1_with_return, task1_with_task_return]:
            with self.subTest():
                c = chain(task.s(), task2needs.s())
                verify_chain_arguments(c)

        @test_app.task(base=FireXTask)
        def task1_no_return():
            pass  # pragma: no cover

        c = chain(task1_no_return.s(stuff="yep"), task2needs.s())
        verify_chain_arguments(c)

        # todo: add this check to the validation
        # with self.assertRaises(InvalidChainArgsException):
        #     c = chain(task1_no_return.s(), task2needs.s())
        #     verify_chain_arguments(c)

        with self.assertRaises(InvalidChainArgsException):
            c = chain(task1_no_return.s(thing="@stuff"), task2needs.s())
            verify_chain_arguments(c)

        with self.assertRaises(InvalidChainArgsException):
            c = chain(task1_no_return.s(), task2needs.s(thing="@stuff"))
            verify_chain_arguments(c)

    def test_arg_properties(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask)
        @returns("take_this")
        def a_task(required, *other_optional_args, optional="yup", **other_optional_kwargs):
            pass  # pragma: no cover

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, returns=['take_this'])
        def b_task(required, *other_optional_args, optional="yup", **other_optional_kwargs):
            pass  # pragma: no cover

        for task in [a_task, b_task]:
            with self.subTest():
                self.assertEqual(task.optional_args, {'optional': "yup"})
                self.assertEqual(task.required_args, ['required'])
                self.assertEqual(task.return_keys, ('take_this',))
                self.assertEqual(task.optional_args, {'optional': "yup"})  # repeat

    def test_chain_assembly_validation(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask)
        @returns("final")
        def beginning(start):
            return "pass"  # pragma: no cover

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, returns='final')
        def beginning2(start):
            return "pass"  # pragma: no cover

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask, returns=FireXTask.DYNAMIC_RETURN)
        def beginning3(start):
            return "pass"  # pragma: no cover

        @test_app.task(base=FireXTask)
        @returns("mildly_amusing")
        def middle_task(very_important):
            assert very_important  # pragma: no cover

        @test_app.task(base=FireXTask, returns=('mildly_amusing',))
        def middle_task2(very_important):
            assert very_important  # pragma: no cover

        @test_app.task(base=FireXTask, returns=FireXTask.DYNAMIC_RETURN)
        def middle_task3(very_important):
            assert very_important  # pragma: no cover

        @test_app.task(base=FireXTask)
        @returns("finished")
        def ending(final, missing):
            assert final, missing  # pragma: no cover

        @test_app.task(base=FireXTask, returns='finished')
        def ending2(final, missing):
            assert final, missing  # pragma: no cover

        @test_app.task(base=FireXTask, returns=FireXTask.DYNAMIC_RETURN)
        def ending3(final, missing):
            assert final, missing  # pragma: no cover

        for b, m, e in [[beginning, middle_task, ending], [beginning2, middle_task2, ending2]]:
            with self.subTest():
                with self.assertRaises(InvalidChainArgsException):
                    c = b.s(start="something") | m.s(very_important="oh_it_is") | e.s()
                    verify_chain_arguments(c)

            with self.subTest():
                c = b.s(start="something") | m.s(very_important="oh_it_is") | e.s(missing="not_missing")
                verify_chain_arguments(c)
                self.assertIsNotNone(chain)

            with self.subTest():
                with self.assertRaises(InvalidChainArgsException):
                    c2 = b.s(start="something") | m.s(very_important="@not_there") | e.s(missing="not missing")
                    verify_chain_arguments(c2)

            with self.subTest():
                c2 = b.s(start="something") | m.s(very_important="@final") | e.s(missing="not missing")
                verify_chain_arguments(c2)
                self.assertIsNotNone(c2)

        with self.subTest():
            c = beginning.s(start='something') | middle_task3.s()
            with self.assertRaises(InvalidChainArgsException):
                verify_chain_arguments(c)

        with self.subTest():
            c = beginning3.s(start='something') | middle_task.s()
            verify_chain_arguments(c)

        with self.subTest():
            c = beginning3.s(start='something') | middle_task3.s() | ending.s()
            verify_chain_arguments(c)

        with self.subTest():
            c = beginning3.s() | middle_task3.s()
            with self.assertRaises(InvalidChainArgsException):
                verify_chain_arguments(c)


class TaskUtilTests(unittest.TestCase):
    def test_get_attr_unwrapped(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task()
        @returns("stuff")
        def fun2():
            pass  # pragma: no cover
        self.assertEqual(get_attr_unwrapped(fun2, "_decorated_return_keys"), ("stuff",))


class InjectArgsTest(unittest.TestCase):
    def test_inject_irrelevant(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        def injected_task1():
            pass  # pragma: no cover

        # inject unneeded things
        with self.subTest("Inject nothing"):
            kwargs = {}
            c = InjectArgs(**kwargs)
            c = c | injected_task1.s()
            verify_chain_arguments(c)

        with self.subTest("Inject nothing useful"):
            kwargs = {"random": "thing"}
            c = InjectArgs(**kwargs)
            c = c | injected_task1.s()
            verify_chain_arguments(c)

        with self.subTest("Inject will be overridden by signature"):
            kwargs = {"injected": "thing"}
            c = InjectArgs(**kwargs)
            c = c | injected_task1.s(injected="stuff")
            self.assertEqual(c.kwargs['injected'], "stuff")

        with self.subTest("Inject will be overridden by chain"):
            kwargs = {"injected": "thing"}
            c = InjectArgs(**kwargs)
            c = c | (injected_task1.s(injected="stuff") | injected_task1.s())
            self.assertEqual(c.tasks[0].kwargs['injected'], "stuff")

    def test_inject_necessary(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        # noinspection PyUnusedLocal
        @test_app.task(base=FireXTask)
        def injected_task2(needed):
            pass  # pragma: no cover

        with self.subTest("Inject directly"):
            c = InjectArgs(needed='stuff', **{})
            c = c | injected_task2.s()
            verify_chain_arguments(c)

        kwargs = {"needed": "thing"}
        with self.subTest("Inject with kwargs"):
            c = InjectArgs(not_needed='stuff', **kwargs)
            c = c | injected_task2.s()
            verify_chain_arguments(c)

        @test_app.task(base=FireXTask)
        def injected_task3():
            pass  # pragma: no cover

        with self.subTest("Injected chained with another"):
            c = InjectArgs(**kwargs)
            c |= injected_task3.s()
            c = chain(c, injected_task2.s())
            verify_chain_arguments(c)

        with self.subTest("Inject into existing chain"):
            c = InjectArgs(**kwargs)
            n_c = injected_task3.s() | injected_task2.s()
            c = c | n_c
            verify_chain_arguments(c)

        with self.subTest("Inject into existing chain"):
            c = InjectArgs(**kwargs)
            c = c | (injected_task3.s() | injected_task2.s())
            verify_chain_arguments(c)

        with self.subTest("Inject into Another inject"):
            c = InjectArgs(**kwargs)
            c |= InjectArgs(sleep=None)
            c |= injected_task2.s()


class LabelTests(unittest.TestCase):
    def test_labels(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        def task1():
            pass  # pragma: no cover

        @test_app.task(base=FireXTask)
        def task2():
            pass  # pragma: no cover

        with self.subTest('InjectArgs with one task and default label'):
            c = InjectArgs() | task1.s()
            self.assertEqual(c.get_label(), task1.name)

        with self.subTest('One Task with default label'):
            c = task1.s()
            self.assertEqual(c.get_label(), task1.name)

        with self.subTest('Two Tasks with default label'):
            c = task1.s() | task2.s()
            self.assertEqual(c.get_label(), '|'.join([task1.name, task2.name]))

        with self.subTest('InjectArgs with one task and label'):
            c = InjectArgs() | task1.s()
            label = 'something'
            c.set_label(label)
            self.assertEqual(c.get_label(), label)

        with self.subTest('One task with label'):
            c = task1.s()
            label = 'something'
            c.set_label(label)
            self.assertEqual(c.get_label(), label)

        with self.subTest('Two tasks with label'):
            c = task1.s() | task2.s()
            label = 'something'
            c.set_label(label)
            self.assertEqual(c.get_label(), label)


class SetExecutionOptionsTests(unittest.TestCase):

    def test(self):
        test_app = FireXCelery.create_ut_fx_celery(sys.modules[__name__].__package__)

        @test_app.task(base=FireXTask)
        def task1():
            pass  # pragma: no cover

        @test_app.task(base=FireXTask)
        def task2():
            pass  # pragma: no cover

        with self.subTest('Testing set_execution_options'):
            options = {'k1': 1, 'k2': 'v2'}

            t = task1.s()
            t.set_execution_options(**options)
            self.assertDictEqual(t['options'], options)

            c1 = task1.s() | task2.s()
            c2 = task2.s() | task1.s()
            for c in [c1, c2]:
                c.set_execution_options(**options)
                for task in c.tasks:
                    self.assertDictEqual(task['options'], options)

        with self.subTest('Testing set_priority'):
            priority = 3
            t = task1.s()
            t.set_priority(priority)
            self.assertEqual(t['options']['priority'], priority)

            c1 = task1.s() | task2.s()
            c2 = task2.s() | task1.s()
            for c in [c1, c2]:
                c.set_priority(priority)
                for task in c.tasks:
                    self.assertEqual(task['options']['priority'], priority)

        with self.subTest('Testing set_queue'):
            queue = 'some_queue'
            t = task1.s()
            t.set_queue(queue)
            self.assertEqual(t['options']['queue'], queue)

            c1 = task1.s() | task2.s()
            c2 = task2.s() | task1.s()
            for c in [c1, c2]:
                c.set_queue(queue)
                for task in c.tasks:
                    self.assertEqual(task['options']['queue'], queue)

        with self.subTest('Testing set_soft_time_limit'):
            soft_time_limit = 60
            t = task1.s()
            t.set_soft_time_limit(soft_time_limit)
            self.assertEqual(t['options']['soft_time_limit'], soft_time_limit)

            c1 = task1.s() | task2.s()
            c2 = task2.s() | task1.s()
            for c in [c1, c2]:
                c.set_soft_time_limit(soft_time_limit)
                for task in c.tasks:
                    self.assertEqual(task['options']['soft_time_limit'], soft_time_limit)