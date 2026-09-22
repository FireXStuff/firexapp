"""
Tests for chain._merged_python_signature, which extends an overriding service's
signature with the args of the services it overrides.
"""

import inspect

from firexkit.bag_of_goodies import AutoInject
from firexkit.chain import _merged_python_signature


class _FakeTask:
    """Just the two attributes _merged_python_signature reads off a FireXTask."""

    def __init__(self, run, orig=None):
        self.run = run
        self.sig = inspect.signature(run)
        self.orig = orig


def _merged(run, *orig_runs) -> inspect.Signature:
    orig_task = None
    for orig_run in reversed(orig_runs):
        orig_task = _FakeTask(orig_run, orig=orig_task)
    return _merged_python_signature(_FakeTask(run, orig=orig_task))


def _params(sig: inspect.Signature) -> dict[str, inspect.Parameter]:
    return dict(sig.parameters)


def _defaults(sig: inspect.Signature) -> dict:
    return {
        name: (None if param.default is param.empty else param.default)
        for name, param in sig.parameters.items()
    }


class TestNoOverride:
    def test_signature_is_unchanged(self):
        def task(a, b=1, **kwargs):
            pass  # pragma: no cover

        assert _merged(task) == inspect.signature(task)

    def test_missing_sig_attribute_falls_back_to_run(self):
        def task(a):
            pass  # pragma: no cover

        class NoSigTask:
            run = staticmethod(task)

        assert _merged_python_signature(NoSigTask()) == inspect.signature(task)


class TestAddedArgs:
    def test_orig_only_arg_is_added_as_optional_keyword_only(self):
        def task(a):
            pass  # pragma: no cover

        def orig(a, b):
            pass  # pragma: no cover

        merged = _merged(task, orig)
        assert list(merged.parameters) == ["a", "b"]
        added = merged.parameters["b"]
        # the overriding service doesn't declare it, so it can't be mandatory for it.
        assert added.default is None
        # keyword-only is always a valid position, and chain args are always by name.
        assert added.kind == added.KEYWORD_ONLY

    def test_added_arg_keeps_orig_default_and_annotation(self):
        def task(a):
            pass  # pragma: no cover

        def orig(a, b: int = 7):
            pass  # pragma: no cover

        added = _merged(task, orig).parameters["b"]
        assert added.default == 7
        assert added.annotation is int

    def test_added_args_go_before_var_keyword(self):
        # this raised 'wrong parameter order' before, abandoning the whole merge.
        def task(a, **kwargs):
            pass  # pragma: no cover

        def orig(a, b):
            pass  # pragma: no cover

        merged = _merged(task, orig)
        assert list(merged.parameters) == ["a", "b", "kwargs"]
        assert merged.parameters["kwargs"].kind == inspect.Parameter.VAR_KEYWORD

    def test_added_args_go_after_var_positional(self):
        def task(a, *args):
            pass  # pragma: no cover

        def orig(a, b):
            pass  # pragma: no cover

        assert list(_merged(task, orig).parameters) == ["a", "args", "b"]

    def test_orig_var_args_are_not_added(self):
        # replacing their default raised 'cannot have default values' before.
        def task(a):
            pass  # pragma: no cover

        def orig(a, *args, **kwargs):
            pass  # pragma: no cover

        assert list(_merged(task, orig).parameters) == ["a"]

    def test_auto_inject_args_are_not_added(self):
        def task(a):
            pass  # pragma: no cover

        def orig(a, injected: AutoInject[int] = None):
            pass  # pragma: no cover

        assert list(_merged(task, orig).parameters) == ["a"]

    def test_transitive_orig_args_are_added(self):
        def task(a):
            pass  # pragma: no cover

        def orig(a, b):
            pass  # pragma: no cover

        def orig_orig(a, c):
            pass  # pragma: no cover

        assert list(_merged(task, orig, orig_orig).parameters) == ["a", "b", "c"]

    def test_nearest_orig_wins_for_an_added_arg(self):
        def task(a):
            pass  # pragma: no cover

        def orig(a, b=1):
            pass  # pragma: no cover

        def orig_orig(a, b=2):
            pass  # pragma: no cover

        assert _merged(task, orig, orig_orig).parameters["b"].default == 1


class TestAdoptedDefaults:
    def test_default_is_adopted_when_the_order_allows_it(self):
        def task(a, b=1):
            pass  # pragma: no cover

        def orig(a=3, b=1):
            pass  # pragma: no cover

        assert _defaults(_merged(task, orig)) == {"a": 3, "b": 1}

    def test_default_is_adopted_for_every_arg_of_a_positional_run(self):
        def task(a, b, c=1):
            pass  # pragma: no cover

        def orig(a=3, b=4, c=1):
            pass  # pragma: no cover

        assert _defaults(_merged(task, orig)) == {"a": 3, "b": 4, "c": 1}

    def test_default_is_skipped_when_it_would_break_the_order(self):
        # 'non-default argument follows default argument': b has no default to adopt,
        # so a can't take one either, but c is still free to.
        def task(a, b, c):
            pass  # pragma: no cover

        def orig(a=3, c=5, added=6):
            pass  # pragma: no cover

        merged = _merged(task, orig)
        # the rest of the merge still happens.
        assert _defaults(merged) == {"a": None, "b": None, "c": 5, "added": 6}

    def test_no_default_is_adopted_when_a_later_arg_stays_mandatory(self):
        def task(a, b):
            pass  # pragma: no cover

        # only keyword-only args can be mandatory after a defaulted one.
        def orig(*, a=3, b):
            pass  # pragma: no cover

        assert _defaults(_merged(task, orig)) == {"a": None, "b": None}

    def test_positional_only_run_is_independent_of_the_rest(self):
        def task(a, /, b):
            pass  # pragma: no cover

        def orig(a=3, b=4):
            pass  # pragma: no cover

        merged = _merged(task, orig)
        # a ends its own kind's run, so its default doesn't depend on b's.
        assert _defaults(merged) == {"a": 3, "b": 4}
        assert merged.parameters["a"].kind == inspect.Parameter.POSITIONAL_ONLY

    def test_keyword_only_args_have_no_ordering_constraint(self):
        def task(*, a, b):
            pass  # pragma: no cover

        def orig(a=3, b=4):
            pass  # pragma: no cover

        assert _defaults(_merged(task, orig)) == {"a": 3, "b": 4}

    def test_own_default_is_not_replaced(self):
        def task(a=1):
            pass  # pragma: no cover

        def orig(a=2):
            pass  # pragma: no cover

        assert _merged(task, orig).parameters["a"].default == 1

    def test_var_args_never_get_a_default(self):
        def task(*args, **kwargs):
            pass  # pragma: no cover

        def orig(args=1, kwargs=2):
            pass  # pragma: no cover

        merged = _merged(task, orig)
        assert all(p.default is p.empty for p in merged.parameters.values())

    def test_nearest_orig_wins_for_an_adopted_default(self):
        def task(a):
            pass  # pragma: no cover

        def orig(a=3):
            pass  # pragma: no cover

        def orig_orig(a=4):
            pass  # pragma: no cover

        assert _merged(task, orig, orig_orig).parameters["a"].default == 3

    def test_auto_inject_args_keep_their_own_default(self):
        # auto-inject args must not get defaults from an override.
        def task(injected: AutoInject[int]):
            pass  # pragma: no cover

        def orig(injected: AutoInject[int] = 3):
            pass  # pragma: no cover

        assert (
            _merged(task, orig).parameters["injected"].default
            is inspect.Parameter.empty
        )


class TestKindsAndOrderAreValid:
    def test_everything_merged_at_once(self):
        def task(pos, /, a, *args, kw_only, **kwargs):
            pass  # pragma: no cover

        def orig(pos=1, a=2, added=3, kw_only=4, also_added=5):
            pass  # pragma: no cover

        merged = _merged(task, orig)
        assert list(merged.parameters) == [
            "pos",
            "a",
            "args",
            "kw_only",
            "added",
            "also_added",
            "kwargs",
        ]
        assert _defaults(merged) == {
            "pos": 1,
            "a": 2,
            "args": None,
            "kw_only": 4,
            "added": 3,
            "also_added": 5,
            "kwargs": None,
        }
        # the merged params must describe a callable python accepts.
        assert _params(inspect.Signature(list(merged.parameters.values()))) == _params(
            merged
        )
