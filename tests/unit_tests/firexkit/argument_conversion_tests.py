
import unittest
from celery import Celery

from firexkit.argument_conversion import ConverterRegister, CircularDependencyException, \
    MissingConverterDependencyError, ConverterRegistrationException, NameDuplicationException, SingleArgDecorator, \
    ArgumentConversionException
from firexkit.task import FireXTask


class ArgConversionTests(unittest.TestCase):

    def test_converter_registration(self):
        test_input_converter = ConverterRegister()

        @test_input_converter.register
        def converter_no_dependency(kwargs):
            kwargs['converter_no_dependency'] = True
            return kwargs

        @test_input_converter.register('converter_no_dependency')
        def converter_str_dependency(kwargs):
            kwargs['converter_str_dependency'] = True
            return kwargs

        @test_input_converter.register('converter_no_dependency',
                                       'converter_str_dependency')
        def converter_list_dependency(kwargs):
            kwargs['converter_list_dependency'] = True
            return kwargs

        converted = test_input_converter.convert(**{})
        self.assertTrue('converter_no_dependency' in converted)
        self.assertTrue('converter_str_dependency' in converted)
        self.assertTrue('converter_list_dependency' in converted)

        with self.assertRaises(MissingConverterDependencyError):
            @test_input_converter.register('Nope')
            def missing_dependent(_):
                # Should not reach here
                pass  # pragma: no cover
            test_input_converter.convert(**{})

    def test_converter_dependency(self):
        unit_test_obj = self

        test_input_converter = ConverterRegister()

        @test_input_converter.register
        def converter_one(kwargs):
            kwargs['converter_one'] = True
            return kwargs

        @test_input_converter.register('converter_one')
        def converter_two(kwargs):
            unit_test_obj.assertTrue('converter_one' in kwargs)
            kwargs['converter_two'] = True
            return kwargs

        @test_input_converter.register('converter_four')
        def converter_three(kwargs):
            unit_test_obj.assertTrue('converter_four' in kwargs)
            kwargs['converter_three'] = True
            return kwargs

        @test_input_converter.register
        def converter_four(kwargs):
            kwargs['converter_four'] = True
            return kwargs

        ############################
        # test multiple dependencies
        @test_input_converter.register('converter_one',
                                       'converter_two',
                                       'converter_three',
                                       'converter_four')
        def converter_five(kwargs):
            unit_test_obj.assertTrue('converter_one' in kwargs)
            unit_test_obj.assertTrue('converter_two' in kwargs)
            unit_test_obj.assertTrue('converter_three' in kwargs)
            unit_test_obj.assertTrue('converter_four' in kwargs)
            return kwargs

        test_input_converter.convert(**{})

        #######################################
        # test detection of circular dependency
        test_input_converter = ConverterRegister()
        with self.assertRaises(CircularDependencyException):
            @test_input_converter.register('converter_seven')
            def converter_six(_):
                # Should not reach here
                pass  # pragma: no cover

            @test_input_converter.register('converter_eight')
            def converter_seven(_):
                # Should not reach here
                pass  # pragma: no cover

            @test_input_converter.register('converter_six')
            def converter_eight(_):
                # Should not reach here
                pass  # pragma: no cover
            test_input_converter.convert(**{})

        ################################
        # test unrecognized dependencies
        test_input_converter = ConverterRegister()
        with self.assertRaises(MissingConverterDependencyError):
            @test_input_converter.register("this_is_not_valid")
            def converter_unrecognised(_):
                pass  # Should not reach here # pragma: no cover
            test_input_converter.convert(**{})

        #####################################################
        # test in combination with boolean to indicate pre or post task
        test_input_converter = ConverterRegister()

        @test_input_converter.register(True)
        def converter_nine(kwargs):
            kwargs['converter_nine'] = True

        @test_input_converter.register(False)
        def converter_ten(kwargs):
            kwargs['converter_ten'] = True

        @test_input_converter.register(False, "converter_ten")
        def converter_eleven(kwargs):
            kwargs['converter_eleven'] = True
            unit_test_obj.assertTrue('converter_ten' in kwargs)

        @test_input_converter.register("converter_eleven", False, "converter_ten")
        def converter_twelve(kwargs):
            unit_test_obj.assertTrue('converter_ten' in kwargs)
            unit_test_obj.assertTrue('converter_eleven' in kwargs)

        test_input_converter.convert(**{})
        test_input_converter.convert(pre_task=False, **{})

        #####################################################
        # test pre cannot be dependant on post
        test_input_converter = ConverterRegister()

        @test_input_converter.register(True)
        def converter_thirteen(kwargs):
            kwargs['converter_thirteen'] = True

        # post can be dependant on pre
        @test_input_converter.register(False, "converter_thirteen")
        def converter_fourteen(kwargs):
            unit_test_obj.assertTrue('converter_thirteen' in kwargs)
        kw = test_input_converter.convert(pre_task=True, **{})
        test_input_converter.convert(pre_task=False, **kw)

        @test_input_converter.register(True, "converter_fourteen")
        def converter_fifteen(_):
                # Should not reach here
                pass  # pragma: no cover
        with self.assertRaises(MissingConverterDependencyError):
            test_input_converter.convert(pre_task=True, **{})

        #####################################################
        # test pre cannot be dependant on post
        test_input_converter = ConverterRegister()
        with self.assertRaises(CircularDependencyException):
            @test_input_converter.register("converter_sixteen")
            def converter_sixteen(_):
                # Should not reach here
                pass  # pragma: no cover
            test_input_converter.convert(pre_task=True, **{})

    def test_exclude_indirect_args(self):
        test_input_converter = ConverterRegister()

        @test_input_converter.register(True)
        def no_indirect(kwargs):
            # indirect args should not be passed to converters
            self.assertTrue("excluded" not in kwargs)
            self.assertTrue("ignored" in kwargs)
            self.assertTrue(kwargs["included"])

        kw = test_input_converter.convert(pre_task=True,
                                          **{
                                              "excluded": "@included",
                                              "included": True,
                                              "ignored": "anything",
                                          })
        self.assertTrue("excluded" in kw)
        self.assertTrue("included" in kw)
        self.assertTrue("ignored" in kw)

        # single arg converter redundantly filters @indirect
        @SingleArgDecorator("filter")
        def boom(_):
            raise Exception("Test Fail")  # pragma: no cover
        boom({"filter": "@ya"})

    def test_failing_converters(self):

        test_app = Celery()

        @test_app.task(base=FireXTask)
        def a_task():
                # Should not reach here
                pass  # pragma: no cover

        with self.assertRaises(ConverterRegistrationException):
            # no Function provided
            ConverterRegister.register_for_task(a_task)(None)

        test_input_converter = ConverterRegister()
        with self.assertRaises(ConverterRegistrationException):
            # no arguments provided
            test_input_converter.register()

        test_input_converter = ConverterRegister()
        with self.assertRaises(ConverterRegistrationException):
            @test_input_converter.register(True, {})  # bad type
            def go_boom(_):
                # Should not reach here
                pass  # pragma: no cover

    def test_single_arg_converter(self):
        test_input_converter = ConverterRegister()

        @test_input_converter.register
        @SingleArgDecorator("hit_this", "this_is_not_there", "skip_this")
        def flip(arg_value):
            return not arg_value

        @test_input_converter.register
        @SingleArgDecorator("ya no")
        def nope(_):
            return None

        data = {
            "hit_this": False,
            "skip_this": "@hit_this",
            "do_not_hit_this": False,
            "ya no": "yes"
        }
        result = test_input_converter.convert(**data)
        self.assertTrue(result["hit_this"])
        self.assertFalse(result["do_not_hit_this"])
        self.assertTrue("this_is_not_there" not in result)
        self.assertTrue(result["skip_this"] == "@hit_this")
        self.assertIsNone(result["ya no"])

        @test_input_converter.register
        @SingleArgDecorator("match")
        def go_boom(_):
            raise NotImplementedError("Go boom")

        with self.assertRaises(ArgumentConversionException):
            test_input_converter.convert(**{"match": True})

        with self.assertRaises(ConverterRegistrationException):
            @test_input_converter.register
            @SingleArgDecorator
            def forgot_brackets(_):
                pass  # pragma: no cover

        with self.assertRaises(ConverterRegistrationException):
            @test_input_converter.register
            @SingleArgDecorator()
            def forgot_the_arg(_):
                pass  # pragma: no cover

    def test_append_to_single_arg_converter(self):
        test_input_converter = ConverterRegister()

        @test_input_converter.register
        @SingleArgDecorator("initial_arg")
        def flip(arg_value):
            return not arg_value
        flip.append("dynamic_arg")

        data = {
            "initial_arg": False,
            "dynamic_arg": False
        }
        result = test_input_converter.convert(**data)
        self.assertTrue(result["initial_arg"])
        self.assertTrue(result["dynamic_arg"])
