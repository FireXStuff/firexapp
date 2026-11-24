"""
Tests for the pydantic arg validation in bag_of_goodies, and for the
annotation resolution in task.py that feeds it.
"""

from __future__ import annotations

import dataclasses
import inspect
import logging
from typing import Annotated

import pydantic
import pytest

from firexkit.bag_of_goodies import (
    _ANNOTATION_VALIDATORS,
    BagOfGoodies,
    FireXBaseBaseModel,
    ValidateArgs,
    _get_annotation_validator,
)
from firexkit.firex_celery import FireXCelery
from firexkit.task import FireXTask, _get_signature_with_resolved_annotations

# NOTE: this module deliberately uses 'from __future__ import annotations' so that
# every annotation below is a string at runtime, reproducing the PEP 563 modules
# that triggered the original mock-validator failure.


class PlainDictSubclass(dict):
    """A type pydantic knows nothing about, like firex_cisco's Script."""

    def __init__(self, required_arg):
        super().__init__(required_arg)


class AModel(FireXBaseBaseModel):
    a: int


class AliasedModel(FireXBaseBaseModel):
    a: Annotated[int | None, pydantic.Field(validation_alias="aa")] = None
    b: int | None = pydantic.Field(default=None, alias="bb")
    c: int | None = pydantic.Field(
        default=None,
        validation_alias=pydantic.AliasChoices("c1", pydantic.AliasPath("c2", 0)),
    )
    d: int | None = None


class AliasedByNameModel(FireXBaseBaseModel):
    model_config = pydantic.ConfigDict(validate_by_name=True)

    a: Annotated[int | None, pydantic.Field(validation_alias="aa")] = None


@dataclasses.dataclass
class ADataclass:
    a: int


def _bog(func, kwargs, pydantic_validate=ValidateArgs.ATTEMPT) -> BagOfGoodies:
    return BagOfGoodies(
        _get_signature_with_resolved_annotations(func),
        (),
        kwargs,
        pydantic_validate=pydantic_validate,
    )


def _validated_args(func, kwargs, pydantic_validate=ValidateArgs.ATTEMPT) -> dict:
    bog = _bog(func, kwargs, pydantic_validate)
    bog.update_validated_args()
    return bog.return_args


class TestResolvedAnnotations:
    def test_str_annotations_are_resolved(self):
        def func(x: list[PlainDictSubclass], y: int):
            pass  # pragma: no cover

        # without eval_str these are the strings 'list[PlainDictSubclass]' and 'int'
        assert inspect.signature(func).parameters["y"].annotation == "int"

        sig = _get_signature_with_resolved_annotations(func)
        assert sig.parameters["x"].annotation == list[PlainDictSubclass]
        assert sig.parameters["y"].annotation is int

    def test_unresolvable_annotation_falls_back(self):
        # an annotation that only exists under typing.TYPE_CHECKING.
        def func(x: OnlyForTypeChecking):  # noqa: F821
            pass  # pragma: no cover

        sig = _get_signature_with_resolved_annotations(func)
        assert sig.parameters["x"].annotation == "OnlyForTypeChecking"

    def test_str_annotation_is_never_given_to_pydantic(self):
        # pydantic would resolve the ForwardRef against firexkit's namespace.
        validator = _get_annotation_validator("list[PlainDictSubclass]")
        assert validator.adapter is None

    def test_unresolved_annotation_does_not_fail_the_task(self):
        def func(x: list[PlainDictSubclass]):
            pass  # pragma: no cover

        value = [PlainDictSubclass({"a": 1})]
        # signature deliberately *not* resolved, as it was before the fix.
        bog = BagOfGoodies(
            inspect.signature(func),
            (),
            {"x": value},
            pydantic_validate=ValidateArgs.REQUIRE,
        )
        bog.update_validated_args()
        assert bog.return_args == {"x": value}


class TestAnnotationClassification:
    @pytest.mark.parametrize(
        "annotation, expect_adapter, expect_is_instance_only",
        [
            (int, True, False),
            (list[int], True, False),
            (AModel, True, False),
            (list[AModel], True, False),
            (ADataclass, True, False),
            (PlainDictSubclass, True, True),
            (list[PlainDictSubclass], True, True),
            (PlainDictSubclass | None, True, True),
            (dict[str, PlainDictSubclass], True, True),
            # a real leaf makes the annotation worth validating
            (int | PlainDictSubclass, True, False),
        ],
    )
    def test_classification(self, annotation, expect_adapter, expect_is_instance_only):
        validator = _get_annotation_validator(annotation)
        assert (validator.adapter is not None) == expect_adapter
        assert validator.is_instance_only == expect_is_instance_only

    def test_validators_are_cached(self):
        _ANNOTATION_VALIDATORS.pop(list[AModel], None)
        first = _get_annotation_validator(list[AModel])
        assert _get_annotation_validator(list[AModel]) is first

    def test_unhashable_annotation_does_not_raise(self):
        class Unhashable(type):
            __hash__ = None

        annotation = Unhashable("Unhashable", (), {})
        cached_count = len(_ANNOTATION_VALIDATORS)
        # the cache can't hold it, but classifying it must still work.
        assert _get_annotation_validator(annotation).is_instance_only
        assert len(_ANNOTATION_VALIDATORS) == cached_count

    def test_only_unmodellable_annotations_are_reported(self, caplog):
        class Plain:
            pass

        class BrokenHook:
            @classmethod
            def __get_pydantic_core_schema__(cls, source, handler):
                raise KeyError("boom")

        with caplog.at_level(logging.DEBUG, logger="firexkit.bag_of_goodies"):
            assert _get_annotation_validator(list[Plain]).is_instance_only
            assert _get_annotation_validator(BrokenHook).adapter is None

        # an isinstance() check is what pydantic makes of any plain class, so
        # reporting it is just noise.
        messages = [r.getMessage() for r in caplog.records]
        assert not any("Plain" in m for m in messages)
        assert any("cannot model" in m and "BrokenHook" in m for m in messages)

    @pytest.mark.parametrize(
        "task_options, expect_classified",
        [
            ({}, False),
            ({"pydantic_validate": ValidateArgs.DISABLED}, False),
            ({"pydantic_validate": ValidateArgs.ATTEMPT}, True),
            ({"pydantic_validate": ValidateArgs.REQUIRE}, True),
        ],
    )
    def test_only_validating_tasks_classify_at_registration(
        self, ut_app: FireXCelery, task_options, expect_classified
    ):
        _ANNOTATION_VALIDATORS.pop(list[ADataclass], None)

        @ut_app.task(base=FireXTask, **task_options)
        def a_task(x: list[ADataclass]):
            pass  # pragma: no cover

        # touching the task registers it.
        assert a_task.sig.parameters["x"].annotation == list[ADataclass]
        assert (list[ADataclass] in _ANNOTATION_VALIDATORS) == expect_classified


class TestValidation:
    def test_conversion_still_happens(self):
        def func(x: int):
            pass  # pragma: no cover

        assert _validated_args(func, {"x": "3"}) == {"x": 3}

    def test_model_annotation_with_dict_value(self):
        # this raised PydanticUserError (a RuntimeError) before the fix, because
        # arbitrary_types_allowed was decided from the value, not the annotation.
        def func(x: AModel):
            pass  # pragma: no cover

        assert _validated_args(func, {"x": {"a": 1}}) == {"x": AModel(a=1)}

    def test_explicit_none_model_arg_is_populated_from_bog(self):
        # this raised KeyError before the fix, because the arg's default was
        # looked up as a field of the model.
        def func(model: AModel | None = None):
            pass  # pragma: no cover

        assert _validated_args(func, {"model": None, "a": 1}) == {
            "model": AModel(a=1),
            "a": 1,
        }

    def test_explicit_none_model_arg_keeps_default_when_bog_lacks_fields(self):
        def func(model: AModel | None = None):
            pass  # pragma: no cover

        assert _validated_args(func, {"model": None}, ValidateArgs.REQUIRE) == {
            "model": None
        }

    def test_dataclass_annotation_with_dict_value(self):
        def func(x: ADataclass):
            pass  # pragma: no cover

        assert _validated_args(func, {"x": {"a": 1}}) == {"x": ADataclass(a=1)}

    def test_is_instance_only_annotation_is_left_alone(self):
        def func(x: list[PlainDictSubclass]):
            pass  # pragma: no cover

        # a JSON round-tripped child result arrives as plain dicts; an isinstance()
        # check could only reject it, so ATTEMPT leaves it untouched.
        value = [{"a": 1}]
        assert _validated_args(func, {"x": value}) == {"x": value}

    def test_is_instance_only_annotation_is_enforced_when_required(self):
        def func(x: list[PlainDictSubclass]):
            pass  # pragma: no cover

        with pytest.raises(ValueError, match="Failed to convert arg x"):
            _validated_args(func, {"x": [{"a": 1}]}, ValidateArgs.REQUIRE)

        value = [PlainDictSubclass({"a": 1})]
        assert _validated_args(func, {"x": value}, ValidateArgs.REQUIRE) == {"x": value}

    def test_unmodellable_annotation_is_skipped(self):
        def func(x: PlainDictSubclass):
            pass  # pragma: no cover

        # no arbitrary_types_allowed escape hatch is needed: the annotation is
        # classified once and simply never validated.
        value = "not a PlainDictSubclass at all"
        assert _validated_args(func, {"x": value}) == {"x": value}

    def test_attempted_validation_failure_warns_instead_of_raising(self):
        def func(x: int):
            pass  # pragma: no cover

        assert _validated_args(func, {"x": "not an int"}) == {"x": "not an int"}

    def test_required_validation_failure_raises_value_error(self):
        def func(x: int):
            pass  # pragma: no cover

        with pytest.raises(ValueError, match="Failed to convert arg x"):
            _validated_args(func, {"x": "not an int"}, ValidateArgs.REQUIRE)

    def test_broken_pydantic_hook_does_not_fail_the_task(self):
        class BrokenHook:
            @classmethod
            def __get_pydantic_core_schema__(cls, source, handler):
                raise KeyError("boom")

        def func(x: BrokenHook):
            pass  # pragma: no cover

        value = object()
        assert _validated_args(func, {"x": value}, ValidateArgs.REQUIRE) == {"x": value}

    def test_disabled_validation_does_nothing(self):
        def func(x: int):
            pass  # pragma: no cover

        assert _validated_args(func, {"x": "3"}, ValidateArgs.DISABLED) == {"x": "3"}


class TestModelUserErrorIsContained:
    def test_model_with_unresolvable_ref_warns_instead_of_raising(self):
        class UnbuiltModel(FireXBaseBaseModel):
            a: NeverDefined  # noqa: F821

        def func(x: UnbuiltModel):
            pass  # pragma: no cover

        # the model itself can't be built, so pydantic raises PydanticUserError
        # (a RuntimeError) rather than a ValidationError.
        with pytest.raises(pydantic.PydanticUserError):
            UnbuiltModel(a=1)

        value = {"a": 1}
        assert _validated_args(func, {"x": value}) == {"x": value}


class TestPydanticConvertibleArgNames:
    """
    The names submit accepts as CLI args because a modelled arg is built from them.
    """

    def test_aliased_fields_are_named_by_their_aliases(self):
        def func(model: AliasedModel | None = None):
            pass  # pragma: no cover

        # pydantic ignores an aliased field's own name unless validate_by_name.
        assert AliasedModel.model_validate({"a": 1}).a is None
        assert AliasedModel.model_validate({"aa": 1}).a == 1

        bog = _bog(func, {})
        assert bog.get_pydantic_convertible_arg_names() == {"aa", "bb", "c1", "c2", "d"}

    def test_validate_by_name_accepts_field_names_too(self):
        def func(model: AliasedByNameModel | None = None):
            pass  # pragma: no cover

        bog = _bog(func, {})
        assert bog.get_pydantic_convertible_arg_names() == {"a", "aa"}

    def test_disabled_validation_converts_nothing(self):
        def func(model: AliasedModel | None = None):
            pass  # pragma: no cover

        bog = _bog(func, {}, ValidateArgs.DISABLED)
        assert bog.get_pydantic_convertible_arg_names() == set()


class TestPostPydanticConvertSuppliedArgNames:
    """
    The names a service's modelled args let the bog produce, which chain
    validation feeds straight back in to BagOfGoodies.update().
    """

    def test_model_fields_are_hoisted_when_the_model_arg_is_supplied(self):
        def func(model: AModel | None = None, a=None):
            pass  # pragma: no cover

        # 'a' is a field of AModel and the arg holding it was supplied.
        bog = _bog(func, {"model": {"a": 1}})
        assert bog.get_post_pydantic_convert_supplied_arg_names() == {"a"}

    def test_nothing_is_hoisted_when_the_model_arg_is_absent(self):
        def func(model: AModel | None = None, a=None, b=None):
            pass  # pragma: no cover

        # nothing can come from 'model' when 'model' itself wasn't supplied, and
        # 'b' isn't one of its fields either way.
        bog = _bog(func, {"x": 1})
        assert bog.get_post_pydantic_convert_supplied_arg_names() == set()

    def test_model_arg_is_convertible_when_its_fields_are_supplied(self):
        def func(model: AModel | None = None, a=None, b=None):
            pass  # pragma: no cover

        bog = _bog(func, {"a": 1, "b": 2})
        assert bog.get_post_pydantic_convert_supplied_arg_names() == {"model"}

    def test_model_arg_is_convertible_when_its_fields_are_supplied_by_alias(self):
        def func(model: AliasedModel | None = None):
            pass  # pragma: no cover

        bog = _bog(func, {"aa": 1, "bb": 2, "c2": [3], "d": 4})
        assert bog.get_post_pydantic_convert_supplied_arg_names() == {"model"}

        # the field names alone don't populate the model.
        bog = _bog(func, {"a": 1, "b": 2, "c": 3, "d": 4})
        assert bog.get_post_pydantic_convert_supplied_arg_names() == set()

    def test_aliased_fields_are_hoisted_by_field_name(self):
        def func(model: AliasedModel | None = None, a=None, aa=None):
            pass  # pragma: no cover

        # hoisting reads the model's attributes, which are named by field.
        bog = _bog(func, {"model": {"aa": 1}})
        assert bog.get_post_pydantic_convert_supplied_arg_names() == {"a"}

    def test_var_keyword_is_never_convertible(self):
        def func(model: AModel | None = None, **kwargs):
            pass  # pragma: no cover

        # 'kwargs' names the container of args, not an arg the bog can produce.
        # Offering it made the update() below raise KeyError: 'kwargs'.
        bog = _bog(func, {"model": {"a": 1}})
        names = bog.get_post_pydantic_convert_supplied_arg_names()
        assert "kwargs" not in names
        bog.update({name: True for name in names})
