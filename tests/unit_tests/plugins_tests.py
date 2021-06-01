import os
import unittest

from firexapp.plugins import identify_duplicate_tasks, find_plugin_file, cdl2list, get_plugin_modules, \
    get_active_plugins, set_plugins_env, load_plugin_modules, get_plugin_module_list, merge_plugins, \
    plugin_support_parser
from firexkit.task import FireXTask


class DuplicateIdentificationTests(unittest.TestCase):
    def test_identify_duplicate_tasks(self):
        all_tasks = ["microservice.tasks.joey",
                     "external.joey"]
        results = identify_duplicate_tasks(all_tasks, ["external"])

        self.assertTrue(len(results) is 1)
        this, that = tuple(results[0])
        self.assertEqual(this, "microservice.tasks.joey")
        self.assertEqual(that, "external.joey")

    def test_identify_duplicate_tasks_no_dups(self):
        all_tasks = ["microservice.tasks.joey",
                     "external.chandler"]
        results = identify_duplicate_tasks(all_tasks, [])
        self.assertTrue(len(results) == 0)

        # We make sure a sub string is not caught
        all_tasks = ["microservice.tasks.joey",
                     "microservice.tasks.joey_different"]
        results = identify_duplicate_tasks(all_tasks, [])
        self.assertTrue(len(results) == 0)

        # Now we reverse the order to make sure the result is the same
        all_tasks = ["microservice.tasks.joey",
                     "microservice.tasks.joey_different"]
        results = identify_duplicate_tasks(all_tasks, [])
        self.assertTrue(len(results) == 0)

    def test_identify_duplicate_tasks_prioritize(self):
        all_tasks = ["microservice.tasks.joey",
                     "external.joey"]
        results = identify_duplicate_tasks(all_tasks, ["external"])
        self.assertTrue(len(results) is 1)
        this, that = tuple(results[0])
        self.assertEqual(this, "microservice.tasks.joey")
        self.assertEqual(that, "external.joey")

        # Switch the priority, different result
        all_tasks = ["microservice.tasks.joey",
                     "external.joey"]
        results = identify_duplicate_tasks(all_tasks, ["microservice.tasks"])
        self.assertTrue(len(results) is 1)
        this, that = tuple(results[0])
        self.assertEqual(this, "external.joey")
        self.assertEqual(that, "microservice.tasks.joey")

        # Now we reverse the order to make sure the result is the same
        all_tasks = ["external.joey",
                     "microservice.tasks.joey"]
        results = identify_duplicate_tasks(all_tasks, ["microservice.tasks"])
        self.assertTrue(len(results) is 1)
        this, that = tuple(results[0])
        self.assertEqual(this, "external.joey")
        self.assertEqual(that, "microservice.tasks.joey")

        # Multiple priority modules. Last is highest priority
        all_tasks = ['celery_queues_tests.success_test_worker', 'RunOnMcAndWorkerTestConfig_mock.success_test_worker']
        results = identify_duplicate_tasks(all_tasks, ['celery_queues_tests', 'RunOnMcAndWorkerTestConfig_mock'])
        this, that = tuple(results[0])
        self.assertEqual(this, "celery_queues_tests.success_test_worker")
        self.assertEqual(that, "RunOnMcAndWorkerTestConfig_mock.success_test_worker")

        # Multiple priority modules. Last is highest priority, even if the order of the tasks is reversed
        all_tasks.reverse()
        results = identify_duplicate_tasks(all_tasks, ['celery_queues_tests', 'RunOnMcAndWorkerTestConfig_mock'])
        this, that = tuple(results[0])
        self.assertEqual(this, "celery_queues_tests.success_test_worker")
        self.assertEqual(that, "RunOnMcAndWorkerTestConfig_mock.success_test_worker")

    def test_identify_dup_of_dup(self):
        all_tasks = ["original.joey",
                     "first.external.joey",
                     "second.external.joey"]
        for x in range(0, 2):
            with self.subTest(str(x)):
                results = identify_duplicate_tasks(all_tasks, ['first.external', 'second.external'])
                self.assertEqual(len(results), 1)
                self.assertEqual(len(results[0]), 3)
                self.assertTrue("original" in results[0][0])
                self.assertTrue("first" in results[0][1])
                self.assertTrue("second" in results[0][2])
            all_tasks.reverse()

    def test_identify_duplicate_tasks_odd(self):
        all_tasks = ["microservice.tasks.joey",
                     "microservice.tasks.joey_different",
                     "microservice.tasks.different_joey",
                     "external.joey"]
        results = identify_duplicate_tasks(all_tasks, ["external"])

        self.assertTrue(len(results) is 1)
        this, that = tuple(results[0])
        self.assertEqual(this, "microservice.tasks.joey")
        self.assertEqual(that, "external.joey")


class ResolvePathTests(unittest.TestCase):
    def test_absolute(self):
        self.assertEqual(__file__, find_plugin_file(__file__))

    def test_relative(self):
        old_cwd = os.getcwd()
        try:
            filename = os.path.basename(__file__)
            os.chdir(os.path.dirname(__file__))
            self.assertEqual(__file__, find_plugin_file(filename))
        finally:
            os.chdir(old_cwd)

    def test_fail_to_find(self):
        with self.assertRaises(FileNotFoundError):
            find_plugin_file("complete/gibberish.py")

    def test_resolve_list(self):
        current_dir = os.path.dirname(__file__)
        files = [os.path.join(current_dir, f) for f in os.listdir(current_dir)]
        self.assertEqual(len(files), len(cdl2list(",".join(files))))
        self.assertEqual([], cdl2list(None))

    def test_get_plugin_modules(self):
        self.assertFalse(get_plugin_modules(None))
        self.assertTrue(self.__module__ in get_plugin_modules(__file__))

        with self.assertRaises(FileNotFoundError):
            self.assertFalse(get_plugin_modules("complete/gibberish.py"))

    def test_plugin_env(self):
        self.assertFalse(get_plugin_module_list(""))
        set_plugins_env("")
        self.assertFalse(load_plugin_modules())
        set_plugins_env(__file__)
        self.assertEqual(get_active_plugins(), __file__)
        load_plugin_modules(__file__)

        from firexapp.engine.celery import app as test_app

        @test_app.task(base=FireXTask)
        def override_me():
            pass  # pragma: no cover

        mock = os.path.join(os.path.dirname(__file__), "data", "plugins", "mock_plugin.py")
        load_plugin_modules(mock)
        # original registration is now pointing to overrider
        self.assertEqual(test_app.tasks['plugins_tests.override_me'],
                         test_app.tasks['mock_plugin.override_me'])
        # there is a reference to the original for use
        self.assertTrue(hasattr(test_app.tasks['plugins_tests.override_me'], "orig"))
        self.assertEqual(test_app.tasks['plugins_tests.override_me'].orig,
                         test_app.tasks['plugins_tests.override_me_orig'])
        self.assertEqual(override_me.name, 'plugins_tests.override_me')

        # name matches preexisting python module
        # noinspection PyUnresolvedReferences
        import subprocess
        sp = os.path.join(os.path.dirname(__file__), "data", "plugins", "subprocess.py")
        load_plugin_modules(sp)

        new = os.path.join(os.path.dirname(__file__), "data", "plugins", "new.py")
        load_plugin_modules(new)


class MergePluginsTests(unittest.TestCase):
    def test_merge_plugins(self):
        with self.subTest('identical plugins'):
            plugins_list_1 = 'a,b,c'
            plugins_list_2 = 'a,b,c'
            merged = ','.join(merge_plugins(plugins_list_1, plugins_list_2))
            self.assertEqual(merged, plugins_list_1)

        with self.subTest('subset of plugins'):
            plugins_list_1 = 'a,b,c'
            plugins_list_2 = 'a,d'
            merged = ','.join(merge_plugins(plugins_list_1, plugins_list_2))
            self.assertEqual(merged, 'b,c,a,d')

        with self.subTest('different plugins'):
            plugins_list_1 = 'a,b,c'
            plugins_list_2 = 'd,e'
            merged = ','.join(merge_plugins(plugins_list_1, plugins_list_2))
            self.assertEqual(merged, plugins_list_1+','+plugins_list_2)

        with self.subTest('first list only'):
            plugins_list_1 = None
            plugins_list_2 = 'd,e'
            merged = ','.join(merge_plugins(plugins_list_1, plugins_list_2))
            self.assertEqual(merged, plugins_list_2)

        with self.subTest('second list only'):
            plugins_list_1 = 'a,b'
            plugins_list_2 = ''
            merged = ','.join(merge_plugins(plugins_list_1, plugins_list_2))
            self.assertEqual(merged, plugins_list_1)

        with self.subTest('second list should override'):
            plugins_list_1 = 'a,b'
            plugins_list_2 = 'b,a'
            merged = ','.join(merge_plugins(plugins_list_1, plugins_list_2))
            self.assertEqual(merged, plugins_list_2)


class CDLActionTest(unittest.TestCase):
    def test_cdla(self):
        arguments, _ = plugin_support_parser.parse_known_args(["--plugins", "p1.py", "--plugins", "p2.py"])
        self.assertEqual(arguments.plugins, "p1.py,p2.py")

    def test_cdla_normalization(self):
        arguments, _ = plugin_support_parser.parse_known_args(["--plugins", "p1.py",
                                                               "--plugins", "p2.py",
                                                               "--plugins", "p1.py"])
        self.assertEqual(arguments.plugins, "p2.py,p1.py")
