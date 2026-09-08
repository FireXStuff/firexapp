import os
import unittest
from unittest.mock import patch

from firexapp.plugins import (
    FxPluginRegistry,
    _get_plugin_module_name,
    _identify_duplicate_tasks,
    get_active_plugins,
    merge_plugins,
    plugin_support_parser,
)
from firexkit.task import FireXTask


class DuplicateIdentificationTests(unittest.TestCase):
    def test_identify_duplicate_tasks(self):
        all_tasks = ["microservice.tasks.joey",
                     "external.joey"]
        results = _identify_duplicate_tasks(all_tasks, ["external"])

        self.assertTrue(len(results) == 1)
        this, that = tuple(results['joey'])
        self.assertEqual(this, "microservice.tasks.joey")
        self.assertEqual(that, "external.joey")

    def test_identify_duplicate_tasks_no_dups(self):
        all_tasks = ["microservice.tasks.joey",
                     "external.chandler"]
        results = _identify_duplicate_tasks(all_tasks, [])
        self.assertTrue(len(results) == 0)

        # We make sure a sub string is not caught
        all_tasks = ["microservice.tasks.joey",
                     "microservice.tasks.joey_different"]
        results = _identify_duplicate_tasks(all_tasks, [])
        self.assertTrue(len(results) == 0)

        # Now we reverse the order to make sure the result is the same
        all_tasks = ["microservice.tasks.joey",
                     "microservice.tasks.joey_different"]
        results = _identify_duplicate_tasks(all_tasks, [])
        self.assertTrue(len(results) == 0)

    def test_identify_duplicate_tasks_prioritize(self):
        all_tasks = ["microservice.tasks.joey",
                     "external.joey"]
        results = _identify_duplicate_tasks(all_tasks, ["external"])
        self.assertTrue(len(results) == 1)
        this, that = tuple(results['joey'])
        self.assertEqual(this, "microservice.tasks.joey")
        self.assertEqual(that, "external.joey")

        # Switch the priority, different result
        all_tasks = ["microservice.tasks.joey",
                     "external.joey"]
        results = _identify_duplicate_tasks(all_tasks, ["microservice.tasks"])
        self.assertTrue(len(results) == 1)
        this, that = tuple(results['joey'])
        self.assertEqual(this, "external.joey")
        self.assertEqual(that, "microservice.tasks.joey")

        # Now we reverse the order to make sure the result is the same
        all_tasks = ["external.joey",
                     "microservice.tasks.joey"]
        results = _identify_duplicate_tasks(all_tasks, ["microservice.tasks"])
        self.assertTrue(len(results) == 1)
        this, that = tuple(results['joey'])
        self.assertEqual(this, "external.joey")
        self.assertEqual(that, "microservice.tasks.joey")

        # Multiple priority modules. Last is highest priority
        all_tasks = ['celery_queues_tests.success_test_worker', 'RunOnMcAndWorkerTestConfig_mock.success_test_worker']
        results = _identify_duplicate_tasks(all_tasks, ['celery_queues_tests', 'RunOnMcAndWorkerTestConfig_mock'])
        this, that = tuple(results['success_test_worker'])
        self.assertEqual(this, "celery_queues_tests.success_test_worker")
        self.assertEqual(that, "RunOnMcAndWorkerTestConfig_mock.success_test_worker")

        # Multiple priority modules. Last is highest priority, even if the order of the tasks is reversed
        all_tasks.reverse()
        results = _identify_duplicate_tasks(all_tasks, ['celery_queues_tests', 'RunOnMcAndWorkerTestConfig_mock'])
        this, that = tuple(results['success_test_worker'])
        self.assertEqual(this, "celery_queues_tests.success_test_worker")
        self.assertEqual(that, "RunOnMcAndWorkerTestConfig_mock.success_test_worker")

    def test_identify_duplicate_tasks_registration_order_tie_break(self):
        # Neither module is a known plugin module: the most recently registered
        # task is the dominant one.
        all_tasks = ["microservice.tasks.joey", "some.imported.module.joey"]
        results = _identify_duplicate_tasks(all_tasks, [])
        self.assertEqual(results['joey'][-1], "some.imported.module.joey")

        all_tasks.reverse()
        results = _identify_duplicate_tasks(all_tasks, [])
        self.assertEqual(results['joey'][-1], "microservice.tasks.joey")

    def test_identify_dup_of_dup(self):
        all_tasks = ["original.joey",
                     "first.external.joey",
                     "second.external.joey"]
        for x in range(2):
            with self.subTest(str(x)):
                results = _identify_duplicate_tasks(all_tasks, ['first.external', 'second.external'])
                self.assertEqual(len(results), 1)
                self.assertEqual(len(results['joey']), 3)
                self.assertTrue("original" in results['joey'][0])
                self.assertTrue("first" in results['joey'][1])
                self.assertTrue("second" in results['joey'][2])
            all_tasks.reverse()

    def test_identify_duplicate_tasks_odd(self):
        all_tasks = ["microservice.tasks.joey",
                     "microservice.tasks.joey_different",
                     "microservice.tasks.different_joey",
                     "external.joey"]
        results = _identify_duplicate_tasks(all_tasks, ["external"])

        self.assertTrue(len(results) == 1)
        this, that = tuple(results['joey'])
        self.assertEqual(this, "microservice.tasks.joey")
        self.assertEqual(that, "external.joey")


class ResolvePathTests(unittest.TestCase):
    def test_absolute(self):
        self.assertEqual(__file__, FxPluginRegistry.find_plugin_file(__file__))

    def test_relative(self):
        old_cwd = os.getcwd()
        try:
            filename = os.path.basename(__file__)
            os.chdir(os.path.dirname(__file__))
            self.assertEqual(__file__, FxPluginRegistry.find_plugin_file(filename))
        finally:
            os.chdir(old_cwd)

    def test_fail_to_find(self):
        with self.assertRaises(FileNotFoundError):
            FxPluginRegistry.find_plugin_file("complete/gibberish.py")

    def test_resolve_list(self):
        current_dir = os.path.dirname(__file__)
        files = [os.path.join(current_dir, f) for f in os.listdir(current_dir) if os.path.isfile(f)]
        self.assertEqual(len(files), len(FxPluginRegistry.resolve_plugin_paths(",".join(files))))
        self.assertEqual([], FxPluginRegistry.resolve_plugin_paths(None))

    def test_get_plugin_modules(self):
        self.assertFalse(FxPluginRegistry.resolve_plugin_paths(None))

        files = FxPluginRegistry.resolve_plugin_paths(__file__)
        self.assertTrue(self.__module__ in [_get_plugin_module_name(f) for f in files])

        with self.assertRaises(FileNotFoundError):
            FxPluginRegistry.resolve_plugin_paths("complete/gibberish.py")

    @patch.dict(os.environ, {'firex_plugins': ''})
    def test_plugin_env(self):
        from firexapp.engine.celery import app as test_app
        plugin_registry = test_app.fx_plugins_reg

        self.assertFalse(get_active_plugins())
        FxPluginRegistry.set_plugins_env("")
        plugin_registry.load_plugin_modules(test_app, get_active_plugins())
        FxPluginRegistry.set_plugins_env(__file__)
        self.assertEqual(get_active_plugins(), __file__)
        plugin_registry.load_plugin_modules(test_app, __file__)

        @test_app.task(base=FireXTask)
        def override_me():
            pass  # pragma: no cover

        mock = os.path.join(os.path.dirname(__file__), "data", "plugins", "mock_plugin.py")
        plugin_registry.load_plugin_modules(test_app, mock)
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
        sp = os.path.join(os.path.dirname(__file__), "data", "plugins", "subprocess.py")
        plugin_registry.load_plugin_modules(test_app, sp)

        new = os.path.join(os.path.dirname(__file__), "data", "plugins", "new.py")
        plugin_registry.load_plugin_modules(test_app, new)

    @patch.dict(os.environ, {'firex_plugins': ''})
    def test_indirectly_imported_plugin_module_overrides(self):
        # A plugin file commonly only imports the module that defines the overriding
        # microservices, so that imported module must be given plugin priority too.
        from firexapp.engine.celery import app as test_app
        plugin_registry = test_app.fx_plugins_reg

        @test_app.task(base=FireXTask)
        def indirect_override_me():
            pass  # pragma: no cover

        plugin = os.path.join(os.path.dirname(__file__), "data", "plugins", "indirect_override_plugin.py")
        priority_module_names = plugin_registry._import_plugin_files(test_app, plugin)
        self.assertIn('indirect_override_defs', priority_module_names)
        # the plugin file's own module outranks the modules it imported
        self.assertEqual(priority_module_names[-1], 'indirect_override_plugin')

        plugin_registry._unregister_duplicate_tasks(test_app, priority_module_names)
        self.assertEqual(test_app.tasks['plugins_tests.indirect_override_me'],
                         test_app.tasks['indirect_override_defs.indirect_override_me'])
        self.assertEqual(test_app.tasks['plugins_tests.indirect_override_me'].orig,
                         test_app.tasks['plugins_tests.indirect_override_me_orig'])


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
