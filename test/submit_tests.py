import unittest
from celery import Celery
from firexapp.application import FireXBaseApp
from firexapp.submit.arguments import get_chain_args, ChainArgException, InputConverter, convert_booleans, \
    find_unused_arguments, whitelist_arguments
from firexkit.argument_conversion import ConverterRegister
from firexkit.task import FireXTask


class SubmitArgsTests(unittest.TestCase):

    def test_no_extra_args(self):
        result = get_chain_args([])
        self.assertEqual({}, result)

    def test_good_args(self):
        result = get_chain_args(['--one', 'one', '--two', 'two', '--three', 'three'])
        self.assertEqual({'one': 'one',
                          'two': 'two',
                          'three': 'three'}, result)

    def test_missing_value(self):
        with self.assertRaises(ChainArgException):
            get_chain_args(['--one', 'one', '--two'])
        with self.assertRaises(ChainArgException):
            get_chain_args(['--one', 'one', '--two', '--three', 'three'])

    def test_missing_key(self):
        with self.assertRaises(ChainArgException):
            get_chain_args(['--one', 'one', 'two'])
        with self.assertRaises(ChainArgException):
            get_chain_args(['--one', 'one', 'two', '--three', 'three'])

    def test_bad_key(self):
        with self.assertRaises(ChainArgException):
            get_chain_args(['--1one', 'one'])

    def test_submit_app_args(self):
        main = FireXBaseApp()
        main.create_arg_parser()

        # make sure the unit tests don't actually exit
        class AppExited(BaseException):
            pass

        with self.subTest("bad other args"):
            def exit_app(_, __):
                raise AppExited()
            main.submit_app.parser.exit = exit_app
            with self.assertRaises(AppExited):
                main.submit_app.process_other_chain_args(args=None,
                                                         other_args=['--one', '--two', 'two'])

        with self.subTest("bad ascii"):
            def error_app(_):
                raise AppExited()
            main.arg_parser.error = error_app
            with self.assertRaises(AppExited):
                main.run(sys_argv=['--pound', '£'])


class InputConversionTests(unittest.TestCase):
    def setUp(self):
        self.old = InputConverter.instance()
        self.was_run = InputConverter.pre_load_was_run

    def tearDown(self):
        InputConverter._global_instance = self.old
        InputConverter.pre_load_was_run = self.was_run

    def test_pre_post_load_argument(self):
        InputConverter._global_instance = ConverterRegister()
        InputConverter.pre_load_was_run = False

        # register with no bool is ok during pre
        @InputConverter.register
        def pre(_):
            pass

        # register with bool is ok during pre
        @InputConverter.register(True)
        def pre_with_bool(_):
            pass

        # Do the pre-load conversion
        InputConverter.convert(pre_load=True, **{})

        # can't run pre a second time
        with self.assertRaises(Exception):
            InputConverter.convert(pre_load=True, **{})

        # This on is ok, because it's marked as post convert
        @InputConverter.register(False)
        def explicit_post_is_ok(_):
            pass

        @InputConverter.register
        def implicit_post_is_ok_too(_):
            pass

        @InputConverter.register("explicit_post_is_ok")
        def implicit_post__with_dependency_is_ok(_):
            pass

        # can't register pre-converter after pre was run
        with self.assertRaises(Exception):
            @InputConverter.register(True)
            def too_late(_):
                pass  # pragma: no cover

        # can't register pre-converter after pre was run, even with dependencies
        with self.assertRaises(Exception):
            @InputConverter.register("early", True)
            def still_too_late(_):
                pass  # pragma: no cover

        # but we can run the post load
        InputConverter.convert(pre_load=False, **{})

    def test_ways_of_registering(self):
        @InputConverter.register
        def early(_):
            pass

        @InputConverter.register("early")
        def after(_):
            pass

        @InputConverter.register(True, "after")
        def last(_):
            pass

        @InputConverter.register("after", True)
        def reverse_last(_):
            pass

        @InputConverter.register(False, "after")
        def after_last(_):
            return {"added": True}

        @InputConverter.register(False)
        def in_the_end(_):
            pass
        InputConverter.convert(pre_load=True, **{})
        self.assertEqual(len(InputConverter.convert(pre_load=False, **{})), 1)

    def test_default_boolean_converter(self):
        self.assertTrue(convert_booleans.__name__ in self.old.get_visit_order(pre_task=True))
        e = []
        f = "random"
        initial = {
            "a": "true",
            "b": "TRUE",
            "c": "false",
            "d": "none",
            "e": e,
            "f": f
        }
        expected = {
            "a": True,
            "b": True,
            "c": False,
            "d": None,
            "e": e,
            "f": f
        }
        for k, v in convert_booleans(initial).items():
            self.assertTrue(v is expected[k])


class ArgumentApplicabilityTests(unittest.TestCase):
    test_app = Celery()

    @test_app.task(base=FireXTask, bind=True)
    def micro_for_args_check_test(self, uid):
        pass

    @property
    def base_kwargs(self):
        return {"chain": "test"}

    def test_not_applicable(self):
        kwargs = self.base_kwargs
        kwargs["uid"] = "valid stuff"
        kwargs["not_applicable"] = "invalid stuff"
        unused = find_unused_arguments(kwargs, ["chain"], self.test_app.tasks)
        self.assertEqual(len(unused), 1)

    def test_only_applicable(self):
        kwargs = {'chain': 'noop',
                  'uid': 'FireX-mdelahou-161215-150725-21939'}
        unused = find_unused_arguments(kwargs, ["chain"], self.test_app.tasks)
        self.assertEqual(len(unused), 0)

    def test_white_list(self):
        kwargs = {'chain': 'noop',
                  'uid': 'FireX-mdelahou-161215-150725-21939',
                  'list_arg': "a list",
                  'str_arg': "a str"}
        whitelist_arguments(["list_arg"])
        whitelist_arguments('str_arg')
        unused = find_unused_arguments(kwargs, ["chain", "anything"], self.test_app.tasks)
        self.assertEqual(len(unused), 0)
