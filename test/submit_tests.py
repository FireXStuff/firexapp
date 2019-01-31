import unittest
from firexapp.application import FireXBaseApp
from firexapp.submit import get_chain_args, ChainArgException


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
