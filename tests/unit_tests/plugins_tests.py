import logging
import os
import unittest
from unittest.mock import patch

from firexapp.plugins import (
    FxPluginRegistry,
    LoadedPlugin,
    _get_plugin_module_name,
    _hash_file,
    _identify_duplicate_tasks,
    _priority_module_names,
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
        plugin_registry.load_plugin_modules(test_app, get_active_plugins(), logging.INFO)
        FxPluginRegistry.set_plugins_env(__file__)
        self.assertEqual(get_active_plugins(), __file__)
        plugin_registry.load_plugin_modules(test_app, __file__, logging.INFO)

        @test_app.task(base=FireXTask)
        def override_me():
            pass  # pragma: no cover

        mock = os.path.join(os.path.dirname(__file__), "data", "plugins", "mock_plugin.py")
        plugin_registry.load_plugin_modules(test_app, mock, logging.INFO)
        # original registration is now pointing to overrider
        plugins_tests_task = test_app.tasks['plugins_tests.override_me']
        self.assertEqual(plugins_tests_task,
                         test_app.tasks['mock_plugin.override_me'])
        # there is a reference to the original for use
        self.assertTrue(hasattr(plugins_tests_task, "orig"))
        self.assertEqual(plugins_tests_task.orig,
                         test_app.tasks['plugins_tests.override_me_orig'])
        self.assertEqual(override_me.name, 'plugins_tests.override_me')

        # name matches preexisting python module
        # noinspection PyUnresolvedReferences
        sp = os.path.join(os.path.dirname(__file__), "data", "plugins", "subprocess.py")
        plugin_registry.load_plugin_modules(test_app, sp, logging.INFO)

        new = os.path.join(os.path.dirname(__file__), "data", "plugins", "new.py")
        plugin_registry.load_plugin_modules(test_app, new, logging.INFO)


class PluginPrecedenceTests(unittest.TestCase):
    """
    The plugin listed last wins, everywhere, for every copy of the task it overrides.
    """

    @patch.dict(os.environ, {'firex_plugins': ''})
    def test_imported_module_does_not_outrank_an_earlier_plugins_override(self):
        """
        A module a plugin file merely imports does not get that plugin's precedence,
        even when it defines tasks of its own. Such modules are routinely shared
        between plugins (firex_cisco's plugins/nxpidt/*), so handing one the precedence
        of whichever plugin happened to import it first would silently outrank the
        genuine overrides of every plugin listed before that one.
        """
        from firexapp.engine.celery import app as test_app
        plugin_registry = test_app.fx_plugins_reg

        plugins_dir = os.path.join(os.path.dirname(__file__), "data", "plugins")
        plugin_registry.load_plugin_modules(
            test_app,
            [
                # Genuinely overrides indirect_override_me...
                os.path.join(plugins_dir, "direct_override_plugin.py"),
                # ...and is listed after it, but only imports a module that happens to
                # define the same short name.
                os.path.join(plugins_dir, "indirect_override_plugin.py"),
            ],
            logging.INFO,
        )

        loaded = plugin_registry._loaded_plugins
        self.assertTrue(all(isinstance(p, LoadedPlugin) for p in loaded))
        priority_names = _priority_module_names(loaded)
        self.assertEqual(
            priority_names[-2:],
            ['direct_override_plugin', 'indirect_override_plugin'],
        )
        # The imported module registered a task, but is not itself a precedence slot.
        self.assertIn('indirect_override_defs.indirect_override_me', test_app.tasks)
        self.assertNotIn('indirect_override_defs', priority_names)

        self.assertEqual(
            test_app.tasks['indirect_override_defs.indirect_override_me'],
            test_app.tasks['direct_override_plugin.indirect_override_me'],
            "an imported module must not outrank a listed plugin's override",
        )

    @patch.dict(os.environ, {'firex_plugins': ''})
    def test_later_plugin_file_overrides_an_earlier_plugins_internal_reference(self):
        """
        A plugin listed later replaces an earlier plugin's task even where that
        earlier plugin references it internally. Intercepting a plugin's own services
        is what listing a test plugin after it is for.

        This is the production failure: ci_plugins/bazel_readiness.py calls the
        _InvokeXrbuildPims it defines, and prio1/bazel_readiness_tests.py was listed
        after it to replace that call, but bazel_readiness' own version ran.
        """
        from firexapp.engine.celery import app as test_app
        from firexkit.chain import SignatureX
        plugin_registry = test_app.fx_plugins_reg

        # Load the local_ref_plugin first (lower precedence), then the competing one
        local_plugin = os.path.join(
            os.path.dirname(__file__),
            "data", "plugins", "local_ref_plugin.py"
        )
        competing_plugin = os.path.join(
            os.path.dirname(__file__),
            "data", "plugins", "competing_helper_plugin.py"
        )

        plugin_registry.load_plugin_modules(test_app, local_plugin, logging.INFO)
        plugin_registry.load_plugin_modules(test_app, competing_plugin, logging.INFO)

        # Verify both versions of shared_helper are registered
        self.assertIn('local_ref_plugin.shared_helper', test_app.tasks)
        self.assertIn('competing_helper_plugin.shared_helper', test_app.tasks)

        self.assertEqual(
            test_app.tasks['local_ref_plugin.shared_helper'],
            test_app.tasks['competing_helper_plugin.shared_helper'],
            'a separate, higher-precedence plugin file must override the earlier '
            "plugin's task",
        )

        # The critical assertion: task_using_helper.s() must reach the overriding
        # plugin's shared_helper, even though it names the one in its own file.
        task_using_helper = test_app.tasks['local_ref_plugin.task_using_helper']
        child_sig = task_using_helper.run()
        # Asserted rather than guarded on: if this stopped being a signature the
        # in-plugin reference check below would silently never run.
        self.assertIsInstance(child_sig, SignatureX)
        self.assertEqual(
            child_sig.task,
            'competing_helper_plugin.shared_helper',
            'an in-plugin reference must still be interceptable by a later plugin',
        )

        # The .orig chain still reaches the overridden implementation.
        self.assertEqual(
            test_app.tasks['competing_helper_plugin.shared_helper'].orig,
            test_app.tasks['local_ref_plugin.shared_helper_orig'],
        )

    @patch.dict(os.environ, {'firex_plugins': ''})
    def test_module_imported_by_a_plugin_is_overridden_by_that_plugin(self):
        """
        The module a plugin file imports ranks below the plugin file itself, so the
        plugin's version wins -- including for the imported module's own internal
        reference to the task.

        This is deliberate, and is the long-standing behaviour: such modules are
        shared between plugins, so giving one plugin precedence for a module it merely
        imported would let it outrank plugins listed after it.
        """
        from firexapp.engine.celery import app as test_app
        from firexkit.chain import SignatureX
        plugin_registry = test_app.fx_plugins_reg

        # Only the importing plugin is loaded; the nested module comes along with it.
        plugin = os.path.join(
            os.path.dirname(__file__), "data", "plugins", "cross_importing_plugin.py"
        )
        plugin_registry.load_plugin_modules(test_app, plugin, logging.INFO)

        imported_name = 'nested_defs.cross_plugin_defs.cross_helper'
        importing_name = 'cross_importing_plugin.cross_helper'
        self.assertIn(imported_name, test_app.tasks)
        self.assertIn(importing_name, test_app.tasks)

        self.assertEqual(test_app.tasks[imported_name], test_app.tasks[importing_name])

        child_sig = test_app.tasks[
            'nested_defs.cross_plugin_defs.task_calling_cross_helper'].run()
        self.assertIsInstance(child_sig, SignatureX)
        self.assertEqual(child_sig.task, importing_name)

    @patch.dict(os.environ, {'firex_plugins': ''})
    def test_plugin_file_reimported_under_another_name_is_still_overridden(self):
        """
        A plugin file is imported under a module name derived from its basename alone,
        so a later plugin importing it by package path executes it a second time as a
        second module and registers a second copy of its tasks. That copy must be
        overridden by the later plugin too.

        This is the production failure: prio1/bazel_pr_ops_mgr_tests.py does 'from
        ci_plugins import bazel_pr_ops' and overrides _SelectPrOps, but
        ci_plugins.bazel_pr_ops._SelectPrOps ran instead of the override.
        """
        from firexapp.engine.celery import app as test_app
        from firexkit.chain import SignatureX
        plugin_registry = test_app.fx_plugins_reg

        plugins_dir = os.path.join(os.path.dirname(__file__), "data", "plugins")
        # Listed in precedence order, exactly as the CLI --plugins arg is.
        plugin_registry.load_plugin_modules(
            test_app,
            [
                os.path.join(plugins_dir, "reimported_defs", "reimported_plugin.py"),
                os.path.join(plugins_dir, "reimporting_plugin.py"),
            ],
            logging.INFO,
        )

        override_name = 'reimporting_plugin.reimported_helper'
        self.assertIn(override_name, test_app.tasks)

        for long_name in [
            # The copy from loading the plugin file itself...
            'reimported_plugin.reimported_helper',
            # ...and the copy from the later plugin importing that same file.
            'reimported_defs.reimported_plugin.reimported_helper',
        ]:
            self.assertIn(long_name, test_app.tasks)
            self.assertEqual(
                test_app.tasks[long_name],
                test_app.tasks[override_name],
                f'{long_name} must be overridden by the plugin listed after it',
            )

        child_sig = test_app.tasks[
            'reimported_defs.reimported_plugin.task_using_reimported_helper'].run()
        self.assertIsInstance(child_sig, SignatureX)
        self.assertEqual(child_sig.task, override_name)


def test_replacement_task_of_a_plugin_task_is_still_from_a_plugin(monkeypatch):
    """
    A plugin overridden by another plugin must still report itself as coming from a
    plugin. Its work is done by a replacement ('_orig') task, and from_plugin is what
    both the 'STARTED:' banner and the task-started-info event Flame renders carry, so
    getting it wrong makes the middle link of a chain look like core code.
    """
    monkeypatch.setenv('firex_plugins', '')
    from firexapp.engine.celery import app as test_app
    plugin_registry = test_app.fx_plugins_reg

    # Named into a module of its own so the chain's precedence doesn't depend on
    # whether an earlier test happened to load this test module as a plugin.
    @test_app.task(base=FireXTask, name='chained_core_defs.chained_override_me')
    def chained_override_me():
        pass  # pragma: no cover

    plugins_dir = os.path.join(os.path.dirname(__file__), 'data', 'plugins')
    plugin_registry.load_plugin_modules(
        test_app,
        # Increasing precedence, in one call: core <- mid <- top.
        [os.path.join(plugins_dir, f'{n}.py')
         for n in ('mid_override_plugin', 'top_override_plugin')],
        logging.INFO,
    )

    dominant = test_app.tasks['top_override_plugin.chained_override_me']
    mid = dominant.orig
    core = mid.orig

    assert (dominant.name, mid.name, core.name) == (
        'top_override_plugin.chained_override_me',
        'mid_override_plugin.chained_override_me_orig',
        'chained_core_defs.chained_override_me_orig_orig',
    )

    assert dominant.from_plugin
    assert mid.from_plugin, \
        'a replacement standing in for a plugin task is still from that plugin'
    assert not core.from_plugin, \
        "core's own version must not be reported as coming from a plugin"

    # What the event stream reports alongside from_plugin, so Flame attributes the
    # flag to the plugin's real task name rather than to the replacement's.
    assert mid.name_without_orig == 'mid_override_plugin.chained_override_me'
    assert core.name_without_orig == 'chained_core_defs.chained_override_me'


def _write_plugin(directory, filename, content):
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, filename)
    with open(path, 'w') as f:
        f.write(content)
    return path


def test_same_plugin_reached_through_two_paths_is_not_an_error(tmp_path, caplog):
    """
    firex_cisco ships ci_plugins both in site-packages and in the workspace, so the
    very same plugin is routinely offered under two absolute paths. That must not be
    reported as a name collision, and the resident module must still be handed back
    so the plugin contributes its tasks and its priority.
    """
    content = 'DUPLICATED = True\n'
    first = _write_plugin(str(tmp_path / 'a'), 'dup_content_plugin.py', content)
    second = _write_plugin(str(tmp_path / 'b'), 'dup_content_plugin.py', content)

    first_mod = FxPluginRegistry.import_plugin_file(first)
    assert first_mod is not None

    with caplog.at_level(logging.DEBUG, logger='firexapp.plugins'):
        second_mod = FxPluginRegistry.import_plugin_file(second)

    # Same code, so the already-resident module is this plugin.
    assert second_mod is first_mod
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING], \
        'identical content reached by another path should not be warned about'
    assert any('identical in content' in r.getMessage() for r in caplog.records)


def test_reloading_the_very_same_plugin_file_is_not_warned_about(tmp_path, caplog):
    """
    Loading a plugin that is already resident under the same path is routine, not a
    problem: script_plugins re-imports every preceding plugin in each forked child,
    and a fork inherits the parent's sys.modules. Nothing is lost, so nothing to warn.
    """
    plugin = _write_plugin(str(tmp_path / 'a'), 'reloaded_plugin.py', 'RELOADED = True\n')

    first_mod = FxPluginRegistry.import_plugin_file(plugin)
    assert first_mod is not None

    with caplog.at_level(logging.DEBUG, logger='firexapp.plugins'):
        # replace=True too, since that is how script_plugins loads them.
        second_mod = FxPluginRegistry.import_plugin_file(plugin, replace=True)

    assert second_mod is first_mod
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING], \
        'reloading the same plugin file should not be warned about'
    assert any('was already imported' in r.getMessage() for r in caplog.records)


def test_different_plugin_with_colliding_name_still_errors(tmp_path, caplog):
    """A genuine name collision is still lost work, so it keeps its error."""
    first = _write_plugin(str(tmp_path / 'a'), 'diff_content_plugin.py', 'VALUE = 1\n')
    second = _write_plugin(str(tmp_path / 'b'), 'diff_content_plugin.py', 'VALUE = 2\n')

    assert FxPluginRegistry.import_plugin_file(first) is not None

    with caplog.at_level(logging.DEBUG, logger='firexapp.plugins'):
        second_mod = FxPluginRegistry.import_plugin_file(second)

    assert second_mod is None
    assert [
        r for r in caplog.records
        if r.levelno == logging.ERROR and 'was NOT imported' in r.getMessage()
    ]


def test_hash_file_of_unreadable_path_is_none(tmp_path):
    assert _hash_file(None) is None
    assert _hash_file(str(tmp_path / 'does_not_exist.py')) is None
    assert _hash_file(str(tmp_path)) is None  # a directory


def _loaded(plugin_file, module_name='m', file_hash='h'):
    return LoadedPlugin(
        plugin_file=plugin_file,
        module_name=module_name,
        plugin_file_hash=file_hash,
    )


def test_is_same_plugin():
    # Same path is the same plugin regardless of hash.
    assert _loaded('/x/p.py', file_hash=None).is_same_plugin(_loaded('/x/p.py', file_hash=None))
    # Different path, same module name and content: the duplicated-install case.
    assert _loaded('/site-packages/p.py').is_same_plugin(_loaded('/ws/p.py'))
    # Same content but a different module name is a different plugin.
    assert not _loaded('/a/p.py', module_name='p').is_same_plugin(
        _loaded('/b/q.py', module_name='q'))
    # Same module name but different content is a genuine collision, not one plugin.
    assert not _loaded('/a/p.py', file_hash='h1').is_same_plugin(
        _loaded('/b/p.py', file_hash='h2'))
    # An unknown hash must never be treated as matching another unknown hash.
    assert not _loaded('/a/p.py', file_hash=None).is_same_plugin(
        _loaded('/b/p.py', file_hash=None))


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
