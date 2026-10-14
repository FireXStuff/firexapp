import inspect

import pytest

from firexkit.bag_of_goodies import BagOfGoodies, _bind_pos_args


def _root(a, b, c, d):
    pass  # pragma: no cover


def _override(a):
    pass  # pragma: no cover


def _override_var_pos(a, *rest):
    pass  # pragma: no cover


ROOT_SIG = inspect.signature(_root)
OVERRIDE_SIG = inspect.signature(_override)


class TestBindPosArgs:
    def test_not_overriding_binds_everything(self):
        bound, extras = _bind_pos_args(ROOT_SIG, (1, 2, 3, 4), None)
        assert bound == {"a": 1, "b": 2, "c": 3, "d": 4}
        assert extras == {}

    def test_not_overriding_too_many_positional_still_raises(self):
        with pytest.raises(TypeError):
            _bind_pos_args(OVERRIDE_SIG, (1, 2), None)

    def test_fewer_args_than_override_params(self):
        bound, extras = _bind_pos_args(OVERRIDE_SIG, (), ROOT_SIG)
        assert bound == {}
        assert extras == {}

    def test_args_match_override_params(self):
        bound, extras = _bind_pos_args(OVERRIDE_SIG, (1,), ROOT_SIG)
        assert bound == {"a": 1}
        assert extras == {}

    @pytest.mark.parametrize(
        "pos_args, expected_extras",
        [
            ((1, 2), {"b": 2}),
            ((1, 2, 3), {"b": 2, "c": 3}),
            ((1, 2, 3, 4), {"b": 2, "c": 3, "d": 4}),
        ],
    )
    def test_extras_named_from_root_task(self, pos_args, expected_extras):
        bound, extras = _bind_pos_args(OVERRIDE_SIG, pos_args, ROOT_SIG)
        assert bound == {"a": 1}
        assert extras == expected_extras

    def test_args_beyond_root_positional_params_are_dropped(self):
        bound, extras = _bind_pos_args(OVERRIDE_SIG, (1, 2, 3, 4, 5, 6), ROOT_SIG)
        assert bound == {"a": 1}
        assert extras == {"b": 2, "c": 3, "d": 4}

    def test_var_positional_override_binds_everything(self):
        bound, extras = _bind_pos_args(
            inspect.signature(_override_var_pos), (1, 2, 3), ROOT_SIG
        )
        assert bound == {"a": 1, "rest": (2, 3)}
        assert extras == {}


class TestBagOfGoodiesPosArgExtras:
    def test_extras_land_in_unaccepted_args(self):
        bog = BagOfGoodies(OVERRIDE_SIG, (1, 2), {}, root_orig_task_sig=ROOT_SIG)
        assert bog.bound_pos_args == {"a": 1}
        assert bog.unaccepted_args == {"b": 2}

    def test_extras_land_in_unaccepted_args_with_prev_task_result(self):
        bog = BagOfGoodies(
            OVERRIDE_SIG,
            ({"unrelated": "x"}, 1, 2),
            {"chain_depth": 1},
            root_orig_task_sig=ROOT_SIG,
        )
        assert bog.bound_pos_args == {"a": 1}
        assert bog.unaccepted_args["b"] == 2
