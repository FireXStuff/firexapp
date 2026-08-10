from typing import Type, Any
import dataclasses
import inspect
import typing
import types
import dataclasses

from celery.utils.log import get_task_logger

from firexkit.result import RETURN_KEYS_KEY

logger = get_task_logger(__name__)


@dataclasses.dataclass
class _FireXArgParameters:
    """ this class is just a bunch fo queries on python inspect.Parameter mapping """
    parameters: types.MappingProxyType[str, inspect.Parameter]

    _has_var_keyword: typing.Optional[bool] = None
    _required_arg_names: typing.Optional[set[str]] = None
    _optional_args_to_default_values: typing.Optional[dict[str, Any]] = None

    def _get_non_var_params(self) -> list[inspect.Parameter]:
        return [
            p for p in self.parameters.values()
            if p.kind not in [p.VAR_POSITIONAL, p.VAR_KEYWORD]
        ]

    def get_required_arg_names(self) -> set[str]:
        if self._required_arg_names is None:
            self._required_arg_names = {
                param.name
                for param in self._get_non_var_params()
                if param.default is param.empty
            }
        return self._required_arg_names

    def get_optional_args_to_default_values(self) -> dict[str, Any]:
        if self._optional_args_to_default_values is None:
            self._optional_args_to_default_values = {
                param.name: param.default
                for param in self._get_non_var_params()
                if param.default is not param.empty
            }
        return self._optional_args_to_default_values

    def is_pos_arg_name(self, arg_name: str) -> bool:
        param = self.parameters.get(arg_name)
        return bool(
            param
            and param.kind in [
                param.POSITIONAL_ONLY,
                param.VAR_POSITIONAL,
            ]
        )

    def is_var_pos_arg(self, arg_name: str) -> bool:
        return bool(
            arg_name in self.parameters
            and self.parameters[arg_name].kind == self.parameters[arg_name].VAR_POSITIONAL
        )

    def accepts_kw_arg_name(self, arg_name: str) -> bool:
        if (
            # auto reg is never supplied as an arg.
            arg_name == AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY
            # the VAR_KEYWORD isn't accepted directly, its
            # contents are.
            or self.is_var_kw_arg_name(arg_name)
        ):
            return False
        if self._accepts_var_keyword():
            return True # accepts all
        return arg_name in self.parameters

    def is_var_kw_arg_name(self, arg_name: str) -> bool:
        arg_param = self.parameters.get(arg_name)
        return bool(
            arg_param and arg_param.kind == arg_param.VAR_KEYWORD
        )

    def _accepts_var_keyword(self) -> bool:
        if self._has_var_keyword is None:
            self._has_var_keyword = any(
                p.kind == p.VAR_KEYWORD for p in self.parameters.values())
        return self._has_var_keyword

    def get_unbound_params(self, supplied_arg_names: set[str]) -> dict[str, inspect.Parameter]:
        return {
            k: v
            for k, v in self.parameters.items()
            if k not in supplied_arg_names
        }


class BagOfGoodies:
    """
        This class attempts to avoid runtime errors by avoiding sending
        arguments to tasks that can't accept them. This is partially necessary
        due to how arguments flow through a chain, but it makes it difficult
        to write correct programs since sending unaccepted arguments
        to a serivce are silently ignored.

        Arguments are "supplied" to tasks either
        directly to a task or from a previous task results if the task is in a
        chain. The arguments that can be "accepted" by a task are defined by python's
        inspect.Signature.parameters. This class partitions "supplied" arguments in to
        three variables:

            bound_pos_args:
                supplied, accepted positional args strutured by "bind"
                (i.e. VAR_POSITIONAL is in a key)

            kwargs:
                supplied, accepted keyword args. These are not bound
                (i.e. VAR_KEYWORD entries are flattened in to kwarg )

            unaccepted_args:
                supplied arguments that are not accepted by the inspect.Signature
                supplied to the constructor, plus special infra keys like
                AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY that must never be sent
                to a task.

        It's expected that bound_pos_args/kwargs/unaccepted_args are mutually
        exclusive.

        This class also resolved indirect key references, e.g. SomeTask.s(arg_name='@other_arg')
        will resolve arg_name to the value in other_arg.

    """
    # Special Char to denote indirect parameter references
    INDIRECT_ARG_CHAR = '@'

    def __init__(
        self,
        sig: inspect.Signature,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ):

        self.fx_params = _FireXArgParameters(sig.parameters)

        # If the first positional argument is a dict and we're in a chain, extract
        # data from previous task results
        if args and isinstance(args[0], dict) and kwargs.get('chain_depth', 0) > 0:
            prev_task_result : dict[str, Any] = dict(args[0])
            # Remove the RETURN_KEYS_KEY entry since results are in prev_task_result
            prev_task_result.pop(RETURN_KEYS_KEY, None)

            # Remove chain prev task results from positional args
            self.bound_pos_args = sig.bind_partial(*args[1:]).arguments
            for k, v in self.bound_pos_args.items():
                if (
                    BagOfGoodies.is_self_indirect_ref(v, k)
                    and k in prev_task_result
                ):
                    self.bound_pos_args[k] = prev_task_result[k]
            resolved_kwargs, unaccepted_args = _resolve_indirect_prev_results_and_split_accepted_args(
                types.MappingProxyType(kwargs),
                types.MappingProxyType(prev_task_result),
                self.fx_params,
                bound_pos_arg_names=set(self.bound_pos_args),
            )
        else:
            self.bound_pos_args = sig.bind_partial(*args).arguments
            resolved_kwargs = dict(kwargs)
            unaccepted_args : dict[str, Any] = {}

        # remove keys from kwargs that are bound by the positional args
        for k, v in dict(resolved_kwargs).items():
            if not self.fx_params.accepts_kw_arg_name(k):
                # include unaccepted_args so it's available down the chain
                unaccepted_args[k] = v
                resolved_kwargs.pop(k, None)
            # remove keys bound by positional args
            if k in self.bound_pos_args:
                resolved_kwargs.pop(k, None)

        self.kwargs = resolved_kwargs
        self.unaccepted_args = unaccepted_args

        # auto-inject reg is an unaccepted_args by definition.
        if ( auto_in_reg := AutoInjectRegistry.get_auto_inject_registry(self.unaccepted_args) ):
            # make future auto-injected args use explicit values if present.
            auto_in_reg.update_auto_inject_args(self.all_supplied_args())
            # add any unbound auto-inject args.
            self.kwargs.update(
                auto_in_reg.get_auto_inject_values(
                    unbound_parameters=self.fx_params.get_unbound_params(
                        set(self.all_supplied_args())
                    )
                )
            )

        self.update({}) # resolve indirect refs for bound_pos_args and kwargs

    def all_supplied_args(self) -> dict[str, Any]:
        # excludes defaults.
        # expected to be mutually exclusive, but this order is safest.
        return self.unaccepted_args | self._get_accepted_supplied_args()

    def update(self, updates: dict[str, Any]):
        for k, v in updates.items():
            if (
                k in self.bound_pos_args
                or self.fx_params.is_pos_arg_name(k)
            ):
                self.bound_pos_args[k] = v
            elif self.fx_params.is_var_kw_arg_name(k):
                try:
                    self.kwargs[k].update(v)
                except TypeError as e:
                    raise ValueError(f'VAR_KEYWORD argument {k} should always be an mapping, not: {v}') from e
            elif self.fx_params.accepts_kw_arg_name(k):
                self.kwargs[k] = v
            else:
                self.unaccepted_args[k] = v

        # update indirect bound_pos_args
        self.bound_pos_args = self.bound_pos_args | self._get_indirect_updates(
            self.bound_pos_args,
            self.all_supplied_args())

        # update indirect kwargs
        self.kwargs = self.kwargs | self._get_indirect_updates(
            self.kwargs | self.get_unsupplied_default_args(),
            self.all_supplied_args())

        # we never resolve indirect in unaccepted_args
        for arg_name in self.unaccepted_args:
            if arg_name in self.kwargs or arg_name in self.bound_pos_args:
                self.unaccepted_args.pop(arg_name)

    @property
    def args(self) -> tuple[Any, ...]:
        args : list[Any] = []
        for k, v in self.bound_pos_args.items():
            if self.fx_params.is_var_pos_arg(k):
                # flatten VAR_POSITIONAL
                args += _validate_var_pos_arg(k, v)
            else:
                args.append(v)

        return tuple(args)

    def _get_accepted_supplied_args(self) -> dict[str, Any]:
        return self.kwargs | self.bound_pos_args

    def get_unsupplied_args(self) -> dict[str, inspect.Parameter]:
        supplied_args = self._get_accepted_supplied_args()
        return {
            param.name: param
            for param in self.fx_params.parameters.values()
            if param.name not in supplied_args
        }

    def get_unsupplied_default_args(self) -> dict[str, Any]:
        return {
            n: p.default
            for n, p in self.get_unsupplied_args().items()
            if p.default != p.empty
        }

    def get_accepted_supplied_and_default_args(self) -> dict[str, Any]:
        return  self._get_accepted_supplied_args() | self.get_unsupplied_default_args()

    @classmethod
    def is_self_indirect_ref(cls, value: str, arg_name: str):
        return cls._get_indirect_key(value) == arg_name

    @classmethod
    def _get_indirect_key(cls, value: Any) -> typing.Optional[str]:
        if value and isinstance(value, str) and value.startswith(cls.INDIRECT_ARG_CHAR):
            return value.removeprefix(cls.INDIRECT_ARG_CHAR)
        return None

    @classmethod
    def resolve_indirect(cls, input_dict: dict[str, Any]) -> dict[str, Any]:
        return input_dict | cls._get_indirect_updates(input_dict, input_dict)

    @classmethod
    def _get_indirect_updates(
        cls,
        # data with indirect keys to resolve.
        args_to_resolve: dict[str, Any],
        #
        # data available for resolutions whose indirect keys
        # should themselves NOT be resolved.
        resolve_data: dict[str, Any],
    ) -> dict[str, Any]:

        args_to_indirect_keys = {}
        for arg_name, arg_val in args_to_resolve.items():
            if ( i_key := cls._get_indirect_key(arg_val) ):
                args_to_indirect_keys[arg_name] = i_key
        indirect_keys_to_arg_names : dict[str, set[str]] = {}
        for arg_name, i_key in args_to_indirect_keys.items():
            if i_key not in indirect_keys_to_arg_names:
                indirect_keys_to_arg_names[i_key] = set()
            indirect_keys_to_arg_names[i_key].add(arg_name)

        cur_done_args_names : set[str] = set()
        indirect_key_updates = {}
        args_names_to_resolve = dict(args_to_indirect_keys)
        # keep resolving until a loop resolves no arg names.
        while set(args_names_to_resolve.keys()) != cur_done_args_names:
            next_resolve_args_to_indirect_keys = {}
            cur_done_args_names = set()
            for arg_name, i_key in args_names_to_resolve.items():
                if i_key in resolve_data:
                    indirect_key_updates[arg_name] = resolve_data[i_key]
                    # check if now that arg_name is set, can anything else be resolved
                    for arg_name_ref in (indirect_keys_to_arg_names.get(arg_name) or []):
                        if arg_name_ref not in cur_done_args_names:
                            next_resolve_args_to_indirect_keys[arg_name_ref] = arg_name
                cur_done_args_names.add(arg_name)
            args_names_to_resolve = next_resolve_args_to_indirect_keys

        return indirect_key_updates

    def get_and_remove_chain_depth(self) -> int:
        vals = []
        for l in [self.unaccepted_args, self.kwargs]:
            if 'chain_depth' in l:
                vals.append(l.pop('chain_depth'))
        if vals:
            return vals[-1]
        return 0

    def get_required_arg_names(self):
        return self.fx_params.get_required_arg_names()

    def get_optional_args_to_default_values(self):
        return self.fx_params.get_optional_args_to_default_values()

    def get_unbound_required_arg_names(self) -> set[str]:
        return self.get_required_arg_names() - set(self.get_accepted_supplied_and_default_args())

    def get_args_to_indirect_value_keys(self) -> dict[str, str]:
        args_to_indirect_value_keys = {}
        for arg_name, value in self._get_accepted_supplied_args().items():
            if ( indirect_name := self._get_indirect_key(value) ):
                args_to_indirect_value_keys[arg_name] = indirect_name
        return args_to_indirect_value_keys

    def get_public_supplied_args(self) -> types.MappingProxyType[str, Any]:
        # all signature accepted and unaccepted args, no defaults.
        return types.MappingProxyType({
            k: v for k, v in self.all_supplied_args().items()
            if k != AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY
        })

    @property
    def return_args(self):
        return self.all_supplied_args()

    def init_auto_inject_registry(self, auto_inject_args: list['AutoInjectSpec']):
        # expected to only be called by the root task.
        assert AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY not in self.unaccepted_args, 'AutoInjectRegistry already initialized'

        # special key is never accepted by definition.
        self.unaccepted_args[AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY] = AutoInjectRegistry.create_auto_in_reg(
            auto_inject_args)

    def get_auto_inject_registry(self) -> 'AutoInjectRegistry':
        return (
            self.unaccepted_args.get(AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY)
            or AutoInjectRegistry.empty_auto_inject_reg()
        )

    @classmethod
    def infra_return_keys(cls) -> set[str]:
        return {RETURN_KEYS_KEY, AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY}

    def has_auto_reg(self) -> bool:
        return AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY in self.all_supplied_args()


def _validate_var_pos_arg(var_pos_name: str, var_pos_value) -> tuple[Any, ...]:
    try:
        return tuple(x for x in var_pos_value)
    except TypeError as e:
        #  Did we update() a VAR_POSITIONAL arg with a non-iterable arg? Don't do that!
        raise ValueError(f'VAR_POSITIONAL argument {var_pos_name} should always be an iterable') from e


def _resolve_indirect_prev_results_and_split_accepted_args(
    kwargs: types.MappingProxyType[str, Any],
    prev_task_result: types.MappingProxyType[str, Any],
    fx_params: _FireXArgParameters, # sig.parameters
    bound_pos_arg_names: set[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_kwargs = dict(kwargs)
    unaccepted_args : dict[str, Any] = {}
    for pt_result_name, pr_result_val in prev_task_result.items():
        # Add previous task results to cur args only if any of
        #   - they're not already bound
        #   - the sig accepts varkeyword (e.g. **kwargs)
        if pt_result_name not in bound_pos_arg_names:
            if fx_params.accepts_kw_arg_name(pt_result_name):
                if (
                    pt_result_name not in resolved_kwargs
                    # if x='@x', and x was present in the original args, we must use it
                    or BagOfGoodies.is_self_indirect_ref(
                        resolved_kwargs[pt_result_name], pt_result_name
                    )
                ):
                    resolved_kwargs[pt_result_name] = pr_result_val
                else:
                    # pt_result_name has more explict value from kwargs, ignore prev result value
                    pass
            else:
                # Otherwise add to result args
                unaccepted_args[pt_result_name] = pr_result_val
    return resolved_kwargs, unaccepted_args

A = typing.TypeVar('A')
AutoInject = typing.Annotated[A, 'FireXAutoInject']

def _get_auto_inject_type(annotation) -> typing.Optional[typing.Type]:
    if (
        typing.get_origin(annotation) is typing.Annotated
        and annotation.__metadata__[0] == 'FireXAutoInject'
    ):
        return annotation.__origin__
    return None


T = typing.TypeVar('T')

@dataclasses.dataclass
class AutoInjectSpec(typing.Generic[T]):
    arg_type: typing.Type[T]
    arg_name: str
    default_value: T
    value: typing.Optional[T] = None


@dataclasses.dataclass(frozen=True)
class AutoInjectRegistry:

    # dynamic auto inject keys/values
    _specs_by_name_and_type: dict[str, dict[Type, AutoInjectSpec]]

    AUTO_IN_REG_ABOG_KEY : typing.ClassVar[str] = '__auto_inject_registry'
    EMPTY : typing.ClassVar[typing.Optional['AutoInjectRegistry']] = None

    @classmethod
    def get_auto_inject_registry(
        cls,
        primary_args: dict[str, Any],
    ) -> typing.Optional['AutoInjectRegistry']:
        return primary_args.get(cls.AUTO_IN_REG_ABOG_KEY)

    @classmethod
    def empty_auto_inject_reg(cls) -> 'AutoInjectRegistry':
        if cls.EMPTY is None:
            cls.EMPTY = AutoInjectRegistry({})
        return cls.EMPTY

    @staticmethod
    def create_auto_in_reg(specs: list[AutoInjectSpec]) -> 'AutoInjectRegistry':
        specs_by_name_and_type: dict[str, dict[Type, AutoInjectSpec]] = {}

        for s in specs:
            if s.value is not None:
                raise ValueError(
                    f'AutoInjectRegistry should only be initialised with default values, but {s.arg_name} has a real value.')
            if s.arg_name not in specs_by_name_and_type:
                specs_by_name_and_type[s.arg_name] = {}
            if s.arg_type not in specs_by_name_and_type[s.arg_name]:
                specs_by_name_and_type[s.arg_name][s.arg_type] = s
            else:
                raise ValueError(f'Duplicate specs for {s.arg_name}[{s.arg_type}]')

        return AutoInjectRegistry(specs_by_name_and_type)

    def get_auto_injectable_arg_names(self) -> set[str]:
        return set(self._specs_by_name_and_type)

    def get(self, name: str, default=None) -> Any:
        if name in self._specs_by_name_and_type:
            specs = list(self._specs_by_name_and_type[name].values())
            assert len(specs) == 1, f'Expected exactly one auto-inject value for {name}, found {len(specs)}'
            return specs[0].value or specs[0].default_value

        if default is not None:
            return default
        return None

    def update_auto_inject_args(self, pos_and_kw_args: dict[str, Any]):
        """
            Update the value in the auto-inject registry so that the nearest ancestor's
            value of an auto-injected arg is used instead of the default or a farther ancestor's
            value.
        """
        for arg_name, arg_val in pos_and_kw_args.items():
            if (
                arg_name in self._specs_by_name_and_type
                and (auto_in_arg := self._get_spec_by_name_and_instance(arg_name, arg_val) )
            ):
                if auto_in_arg.value != arg_val:
                    logger.info(f'Overwriting auto-inject arg {arg_name} with abog value: {arg_val}')
                    auto_in_arg.value = arg_val

    def _get_spec_by_name_and_instance(self, arg_name: str, val: typing.Any) -> typing.Optional[AutoInjectSpec]:
        for t, spec in self._specs_by_name_and_type[arg_name].items():
            if isinstance(val, t):
                return spec
        return None

    def _get_spec_by_name_and_type(self, arg_name: str, _type: Type) -> typing.Optional[AutoInjectSpec]:
        if arg_name not in self._specs_by_name_and_type:
            logger.error(f'AutoInjectRegistry not statically initialized for {arg_name}')
        elif _type not in self._specs_by_name_and_type[arg_name]:
            logger.error(f'AutoInjectRegistry not statically initialized for {arg_name}/[{_type}]')
        else:
            return self._specs_by_name_and_type[arg_name][_type]
        return None

    def get_auto_inject_values(
        self,
        # TODO: could validate bound AutoInject values adhere to AutoInject's type.
        unbound_parameters: typing.Mapping[str, inspect.Parameter],
    ) -> dict[str, typing.Any]:
        auto_inject_kwargs = {}
        possible_auto_injectable_params = {
            k: p
            for k, p in unbound_parameters.items()
            # does the receiving task declare any unbound AutoInject
            # args that should be auto-injected?
            if _get_auto_inject_type(p.annotation)
        }
        for auto_inject_name, param in possible_auto_injectable_params.items():
            if param.default != param.empty:
                # for now don't support service-level defaults since all use-cases require run-level defaults
                # and none additionally require service-level defaults. The point of AutoInject is that
                # service definitions can be written assuming AutoInject is populated with a valide type,
                # to adding a default at the service level confuses that and encourages "always have a default"
                # needless defensive coding.
                raise Exception(f'AutoInject arg {auto_inject_name} has a default value.')

            auto_inject_type = _get_auto_inject_type(param.annotation)
            if auto_inject_type:
                spec = self._get_spec_by_name_and_type(auto_inject_name, auto_inject_type)
                if spec:
                    if spec.value is not None:
                        logger.debug(f'Setting non-default AutoInject {auto_inject_name}')
                        auto_in_v = spec.value
                    else:
                        logger.debug(f'Setting default AutoInject {auto_inject_name}')
                        auto_in_v = spec.default_value
                    auto_inject_kwargs[auto_inject_name] = auto_in_v
            else:
                raise Exception(
                    f'AutoInject arg {auto_inject_name} has no inner type. The "Foo" in AutoInject[Foo] is required.')
        if auto_inject_kwargs:
            logger.debug(f'Auto-Injecting args: {", ".join(auto_inject_kwargs)}')
        return auto_inject_kwargs
