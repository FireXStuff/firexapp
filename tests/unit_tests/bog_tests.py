import inspect
import unittest

from firexkit.bag_of_goodies import BagOfGoodies


class BagTests(unittest.TestCase):
    def test_first_unbound_microservice(self):
        # noinspection PyUnusedLocal
        def func(one, two, three):
            # Should not reach here. We only want the signature
            pass  # pragma: no cover
        sig = inspect.signature(func)

        args = tuple()  # first task of chain gets empty tuple
        kwargs = {'log_level': 'debug',
                  'chain': 'noop',
                  'submission_dir': '~',
                  'plugins': '',
                  'argv': ['~/firex7/firex.py', 'submit', '--chain', 'noop', '--no_email', '--sync'],
                  'unconverted_chain_args': {'disable_blaze': True},
                  'concurrent_runs_limit': 6,
                  'sync': True,
                  'no_email': True,
                  'one': 'me'}
        bog = BagOfGoodies(sig, args, kwargs)

        goodies = bog.get_bag()
        self.assertDictEqual(goodies, kwargs, "bag of goodies should be same as kwargs")
        new_data = {"random": 0}
        kwargs.update(new_data)
        self.assertNotEqual(len(kwargs), len(bog.get_bag()), "Updating kwargs should not affect the bog")
        bog.update(new_data)
        self.assertDictEqual(goodies, kwargs, "bag of goodies should be same as kwargs after update")
        mod_value = {"chain": "something"}
        kwargs.update(mod_value)
        bog.update(mod_value)
        self.assertDictEqual(goodies, kwargs, "bag of goodies should be same as kwargs after update to existing key")

    def test_first_with_explicit_args(self):

        # noinspection PyUnusedLocal
        def a_simple_task(value, value2="unimportant"):
            # Should not reach here. We only want the signature
            pass  # pragma: no cover
        sig = inspect.signature(a_simple_task)
        args = ('nope',)
        kwargs = {}
        bog = BagOfGoodies(sig, args, kwargs)
        self.assertDictEqual(bog.get_bag(),
                             {"value": 'nope'})
        bog.update({"value": 'yup'})
        self.assertDictEqual(bog.get_bag(),
                             {"value": 'yup'})
        a, k = bog.split_for_signature()
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0], 'yup')

    def test_positional_arg_is_a_dict(self):
        # noinspection PyUnusedLocal
        def func(some_key):
            # Should not reach here. We only want the signature
            pass  # pragma: no cover

        sig = inspect.signature(func)

        with self.subTest('positional dict is first arg, but no previous returns'):
            args = ({'k1': 'v1'},)
            kwargs = {}
            bog = BagOfGoodies(sig, args, kwargs, has_returns_from_previous_task=False)
            self.assertDictEqual(bog.get_bag(),
                                 {'some_key': {'k1': 'v1'}})
            a, k = bog.split_for_signature()
            self.assertEqual(len(a), 1)
            self.assertEqual(a[0], {'k1': 'v1'})

        with self.subTest('old value should be trumped by positional dict'):
            old_bog = {'some_key': {'k1': 'wrong_value'},
                       'k2': 'other_value'}
            args = (old_bog, {'k1': 'v1'},)
            kwargs = {}
            bog = BagOfGoodies(sig, args, kwargs)
            self.assertDictEqual(bog.get_bag(),
                                 {'some_key': {'k1': 'v1'},
                                  'k2': 'other_value'})
            a, k = bog.split_for_signature()
            self.assertEqual(len(a), 1)
            self.assertEqual(a[0], {'k1': 'v1'})

    def test_chained_unbound_microservice(self):
        # noinspection PyUnusedLocal
        def func(one, two, three=0):
            # Should not reach here. We only want the signature
            pass  # pragma: no cover

        sig = inspect.signature(func)

        old_bog = {'log_level': 'debug',
                   'chain': 'noop',
                   'submission_dir': '~',
                   'plugins': '',
                   'argv': ['~/firex7/firex.py', 'submit', '--chain', 'noop', '--no_email', '--sync'],
                   'unconverted_chain_args': {'disable_blaze': True},
                   'concurrent_runs_limit': 6,
                   'sync': True,
                   'no_email': True,
                   'one': 'me'}
        args = (old_bog,)
        kwargs = {}
        bog = BagOfGoodies(sig, args, kwargs)

        goodies = bog.get_bag()
        self.assertDictEqual(goodies, old_bog, "bag of goodies should be same as kwargs")
        new_data = {"random": 0}
        old_bog.update(new_data)
        self.assertNotEqual(len(old_bog), len(bog.get_bag()), "Updating old bog should not affect the bog")
        bog.update(new_data)
        self.assertDictEqual(goodies, old_bog, "bag of goodies should be same as old_bog after update")
        mod_value = {"chain": "something"}
        old_bog.update(mod_value)
        bog.update(mod_value)
        self.assertDictEqual(goodies, old_bog, "bag of goodies should be same as old_bog after update to existing key")

    def test_indirect_from_return(self):

        # noinspection PyUnusedLocal
        def ending(final, missing):
            self.assertTrue(True)
        sig = inspect.signature(ending)

        old_bog = {
                    'concurrent_runs_limit': 6,
                    'cc_firex': False, 'log_level': 'debug', 'sync': True, 'start': 'yep', 'group': 'me',
                    'plugins': '~/firex7/flow_tests/argument_validation_tests.py',
                    'no_email': True,
                    'final': 'pass',   # <-- This is what we are testing
                    'submission_dir': '~/firex7',
                    'unconverted_chain_args': {'missing': '@final', 'cc_firex': 'False', 'start': 'yep',
                                               'disable_blaze': True,
                                               'plugins': '~/firex7/flow_tests/'
                                                          'argument_validation_tests.py'},
                    'chain': 'beginning,ending',
                    'missing': '@final',  # <-- This is what we are testing
                    'argv': ['~/firex7/firex.py', 'submit', '--chain', 'beginning,ending', '--start',
                             'yep', '--missing', '@final', '--sync', '--no_email', '--cc_firex', 'False',
                             '--monitor_verbosity', 'none', '--flame_port', '54560', '--flame_timeout', '60',
                             '--disable_blaze', 'True', '--external',
                             '~/firex7/flow_tests/argument_validation_tests.py']}
        args = (old_bog,)
        kwargs = {}
        bog = BagOfGoodies(sig, args, kwargs)

        self.assertTrue('missing' in bog.get_bag())
        self.assertEqual(bog.get_bag()['missing'], 'pass')

        a, k = bog.split_for_signature()
        ending(*a, **k)
        self.assertTrue(len(k), 2)
        self.assertEqual(k['missing'], 'pass')

        # noinspection PyUnusedLocal
        def ending_v2(final, missing, **the_kwargs):
            self.assertTrue(True)
        sig_v2 = inspect.signature(ending_v2)
        bog_v2 = BagOfGoodies(sig_v2, args, kwargs)
        a, k = bog_v2.split_for_signature()
        ending_v2(*a, **k)
        self.assertTrue(len(k), 16)

    def test_indirect_from_default(self):
        def fun(arg_zero,
                arg_one="@do_not_use",
                arg_two="@first_value",
                arg_three="@second_value",
                arg_four="@do_not_use"):
            self.assertEqual('success', arg_zero)
            self.assertEqual('success', arg_one)
            self.assertEqual('success', arg_two)
            self.assertEqual('success', arg_three)
            self.assertEqual('success', arg_four)
        sig = inspect.signature(fun)

        old_bog = {'first_value': 'success'}
        args = (old_bog, "@first_value", "success")
        kwargs = {"second_value": 'success',
                  "do_not_use": "failure",
                  "arg_four": "success"}
        bog = BagOfGoodies(sig, args, kwargs)
        a, k = bog.split_for_signature()
        fun(*a, **k)

    def test_existing_goodies_and_extra(self):
        old_bog = {'geo_loc': 'OTT'}
        args = (old_bog,)
        kwargs = {'expected_module': 'additional_for_plugin_propagation.py'}

        # noinspection PyUnusedLocal
        def from_another_test(expected_module):
            # Should not reach here. We only want the signature
            pass  # pragma: no cover
        sig = inspect.signature(from_another_test)
        bog = BagOfGoodies(sig, args, kwargs)

        combined = {}
        combined.update(old_bog)
        combined.update(kwargs)
        self.assertDictEqual(bog.get_bag(), combined)

    def test_bind_goodies(self):
        old_bog = {'geo_loc': 'OTT'}
        args = (old_bog,)
        kwargs = {'expected_module': 'additional_for_plugin_propagation.py'}
        a_self = self

        class Thing:
            # noinspection PyMethodMayBeStatic
            def a_func(self, expected_module):
                a_self.assertEqual(expected_module, "additional_for_plugin_propagation.py")
        t = Thing()
        sig = inspect.signature(t.a_func)
        bog = BagOfGoodies(sig, args, kwargs)
        a, k = bog.split_for_signature()
        t.a_func(*a, **k)

    def test_pop(self):
        # noinspection PyUnusedLocal
        def some_func(a, b=None):
            # Should not reach here. We only want the signature
            pass  # pragma: no cover
        sig = inspect.signature(some_func)

        with self.subTest('Testing popping from kwargs'):
            bog = BagOfGoodies(sig, (1,), {'b': 2})
            self.assertEqual(bog.pop('b'), 2)
            self.assertNotIn('b', bog.kwargs)
            self.assertNotIn('b', bog.return_args)
            with self.assertRaises(KeyError):
                bog.pop('b')
            self.assertEqual(bog.pop('b', 0), 0)

        with self.subTest('Testing popping from return_args'):
            bog = BagOfGoodies(sig, (1,), {'b': 2, 'c': 3})
            self.assertEqual(bog.pop('c'), 3)
            self.assertNotIn('c', bog.kwargs)
            self.assertNotIn('c', bog.return_args)
            with self.assertRaises(KeyError):
                bog.pop('c')
            self.assertEqual(bog.pop('c', 5), 5)


# noinspection PyUnusedLocal
def func_a(x):
    pass  # pragma: no cover


# noinspection PyUnusedLocal
def func_b(x=None):
    pass  # pragma: no cover


# noinspection PyUnusedLocal
def func_c(**kwargs):
    pass  # pragma: no cover


# noinspection PyUnusedLocal
def func_d(x, **kwargs):
    pass  # pragma: no cover


# noinspection PyUnusedLocal
def func_e(x=None, **kwargs):
    pass  # pragma: no cover


# noinspection PyUnusedLocal
def func_f(*args, x):
    pass  # pragma: no cover


class BogTests(unittest.TestCase):

    sig_func_a = inspect.signature(func_a)
    sig_func_b = inspect.signature(func_b)
    sig_func_c = inspect.signature(func_c)
    sig_func_d = inspect.signature(func_d)
    sig_func_e = inspect.signature(func_e)
    sig_func_f = inspect.signature(func_f)

    def test_passing_exact_requirement_from_bog(self):
        previous_returns = {'x': 1}
        input_args = (previous_returns, )
        for sig in [self.sig_func_a, self.sig_func_b, self.sig_func_c, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, previous_returns)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), previous_returns)

    def test_passing_more_than_exact_requirement_from_bog(self):
        useful_return = {'x': 1}
        nonuseful_return = {'y': 1}
        previous_returns = {**useful_return, **nonuseful_return}
        input_args = (previous_returns, )
        for sig in [self.sig_func_a, self.sig_func_b]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, useful_return)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.return_args, previous_returns)
        for sig in [self.sig_func_c, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, previous_returns)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), previous_returns)

    def test_passing_from_bog_with_an_arg_overwrite(self):
        previous_returns = {'x': 1}
        x_user_overwrite = 2
        input_args = (previous_returns, x_user_overwrite)
        for sig in [self.sig_func_a, self.sig_func_b, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, {})
                self.assertListEqual(args, [x_user_overwrite])
                self.assertDictEqual(bog.get_bag(), {'x': x_user_overwrite})

    def test_passing_from_bog_with_an_kwarg_overwrite(self):
        previous_returns = {'x': 1}
        input_args = (previous_returns,)
        input_kwargs = {'x': 2}
        for sig in [self.sig_func_a, self.sig_func_b, self.sig_func_c, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, input_kwargs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, input_kwargs)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), input_kwargs)

    def test_passing_from_bog_with_an_arg_overwrite_indirect_same_var(self):
        previous_returns = {'x': 1}
        x_user_overwrite = '@x'
        input_args = (previous_returns, x_user_overwrite)
        for sig in [self.sig_func_a, self.sig_func_b, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, {})
                self.assertListEqual(args, [1])
                self.assertDictEqual(bog.get_bag(), {'x': 1})

    def test_passing_from_bog_with_indirect(self):
        previous_returns = {'y': 2}
        new_inputs = {'x': '@y'}
        new_inputs_after_resolving = {'x': 2}
        expected_returns = {**new_inputs_after_resolving, **previous_returns}
        input_args = (previous_returns,)
        for sig in [self.sig_func_a, self.sig_func_b]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, new_inputs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, new_inputs_after_resolving)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), expected_returns)
        for sig in [self.sig_func_c, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, new_inputs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, expected_returns)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), expected_returns)

    def test_passing_from_bog_with_indirect_same_var_name(self):
        previous_returns = {'x': 2}
        new_inputs = {'x': '@x'}
        new_inputs_after_resolving = {'x': 2}
        expected_returns = {**new_inputs_after_resolving, **previous_returns}
        input_args = (previous_returns,)
        for sig in [self.sig_func_a, self.sig_func_b]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, new_inputs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, new_inputs_after_resolving)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), expected_returns)
        for sig in [self.sig_func_c, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, new_inputs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, expected_returns)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), expected_returns)

    def test_passing_from_bog_with_indirect_positional(self):
        previous_returns = {'y': 2}
        expected_returns = {'x': 2, **previous_returns}
        input_args = (previous_returns, '@y')
        for sig in [self.sig_func_a, self.sig_func_b]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, {})
                self.assertListEqual(args, [2])
                self.assertDictEqual(bog.get_bag(), expected_returns)
        for sig in [self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, previous_returns)
                self.assertListEqual(args, [2])
                self.assertDictEqual(bog.get_bag(), expected_returns)

    def test_passing_from_non_bog(self):
        value = 3
        input_args = (value,)
        for sig in [self.sig_func_a, self.sig_func_b, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, {})
                self.assertListEqual(args, [value])
                self.assertDictEqual(bog.get_bag(), {'x': value})

    def test_passing_extra_kwargs(self):
        used_input = {'x': 2}
        input_kwargs = {**used_input, 'y': 3}
        for sig in [self.sig_func_a, self.sig_func_b]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, (), input_kwargs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, used_input)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), input_kwargs)
        for sig in [self.sig_func_c, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, (), input_kwargs)
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, input_kwargs)
                self.assertListEqual(args, [])
                self.assertDictEqual(bog.get_bag(), input_kwargs)

    def test_bog_update(self):
        value = 3
        input_args = (value, )
        for sig in [self.sig_func_a, self.sig_func_b, self.sig_func_d, self.sig_func_e]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, {})
                self.assertListEqual(args, [value])
                self.assertDictEqual(bog.get_bag(), {'x': value})
                new_value = 2
                bog.update({'x': new_value})
                args, kwargs = bog.split_for_signature()
                self.assertListEqual(args, [new_value])
                self.assertDictEqual(kwargs, {})
                self.assertDictEqual(bog.get_bag(), {'x': new_value})

    def test_bog_update_var_positional(self):
        value = 3
        input_args = (1,)
        for sig in [self.sig_func_f]:
            with self.subTest(sig.__str__()):
                bog = BagOfGoodies(sig, input_args, {'x': value})
                args, kwargs = bog.split_for_signature()
                self.assertDictEqual(kwargs, {'x': value})
                self.assertListEqual(args, [1])
                self.assertDictEqual(bog.get_bag(), {'args': (1,), 'x': value})
                new_value = 2
                bog.update({'x': new_value})
                args, kwargs = bog.split_for_signature()
                self.assertListEqual(args, [1])
                self.assertDictEqual(kwargs, {'x': new_value})
                self.assertDictEqual(bog.get_bag(), {'args': (1,), 'x': new_value})

    def test_update_masks_default(self):
        # noinspection PyUnusedLocal
        def something(argument, parameter="default parameter"):
            pass  # pragma: no cover

        bog = BagOfGoodies(inspect.signature(something), ("provided argument",), {})
        updates = {
                "argument": "converter argument",
                "parameter": "converter parameter"  # initially the default value would have been used
            }
        bog.update(updates)
        self.assertEqual(bog.get_bag(), updates)
        self.assertEqual(bog.args, ["converter argument"])
        self.assertEqual(bog.kwargs, {"parameter": "converter parameter"})
