import inspect
from typing import Optional, Any
import types

from celery.canvas import Signature
from celery.result import AsyncResult
from celery.utils.log import get_task_logger

from firexkit.result import wait_on_async_results_and_maybe_raise, FireXResults
from firexkit.bag_of_goodies import BagOfGoodies, AutoInjectRegistry

logger = get_task_logger(__name__)

# this is where most code import from!!
returns = FireXResults.returns


class InjectArgs(Signature):

    def __init__(self, *args, **kwargs):
        assert not args, f'Inject args accepts no positional args.'
        self.args = ()
        self.kwargs = kwargs

    def __str__(self):
        return f'InjectArgs({", ".join(self.kwargs.keys())})'

    @property
    def options(self):
        return dict()

    def __or__(self, other):
        if isinstance(other, InjectArgs):
            r = InjectArgs(**(self.kwargs | other.kwargs))
        else:
            r = other.clone()
            # chains and signatures are both handled by this
            SignatureX.injectArgs(r, **self.kwargs)
        return r


def verify_chain_arguments(sig: 'SignatureX'):
    """
    Verifies that the chain is not missing any parameters. Asserts if any parameters are missing, or if a
    reference parameter (@something) has not provider
    """
    sig.verify_args()


class InvalidChainArgsException(Exception):
    def __init__(self, msg, wrong_args: Optional[dict]=None):
        super(InvalidChainArgsException, self).__init__(msg)
        self.wrong_args = wrong_args or {}


def _simulate_chain_args_kwargs(
    prev_task_return_args : Optional[dict[str, Any]],
    task_pos_args: tuple,
    task_kwargs: dict[str, Any],
    chain_depth: int,
) -> tuple[
    tuple[Any, ...],
    dict[str, Any],
]:
    simulated_pos_args = task_pos_args
    if prev_task_return_args is not None and chain_depth:
        simulated_pos_args = tuple(
            # don't know why, but in a chain the previous task's results are supplied
            # as the fist positional arg. Crazy.
            [ prev_task_return_args ] + list(task_pos_args)
        )
        simulated_kwargs = task_kwargs | dict(chain_depth=chain_depth)
    else:
        simulated_pos_args = task_pos_args
        simulated_kwargs = task_kwargs

    return simulated_pos_args, simulated_kwargs


def _merged_python_signature(task_obj) -> inspect.Signature:
    py_sig = getattr(task_obj, 'sig', inspect.signature(task_obj.run))

    try:
        merged_params : dict[str, inspect.Parameter] = dict(py_sig.parameters)
        orig_task = getattr(task_obj, 'orig', None)
        while orig_task:
            orig_task_py_sig = getattr(orig_task, 'sig', inspect.signature(orig_task.run))
            for orig_arg, orig_param in orig_task_py_sig.parameters.items():
                # auto-inject args must not have defaults since it makes
                # priority resolution too complicated for the user.
                if not BagOfGoodies.get_auto_inject_type(orig_param.annotation):
                    if orig_arg not in merged_params:
                        # hack avoid # non-default argument follows default argument
                        if orig_param.default != orig_param.empty:
                            new_default = orig_param.default
                        else:
                            new_default = None
                        merged_params[orig_arg] = orig_param.replace(default=new_default)
                    else:
                        existing_param = merged_params[orig_arg]
                        if (
                            existing_param.default == existing_param.empty
                            and orig_param.default != orig_param.empty
                        ):
                            merged_params[orig_arg] = existing_param.replace(
                                default=orig_param.default
                            )
            orig_task = getattr(orig_task, 'orig', None)

        return py_sig.replace(
            parameters=list(merged_params.values()),
        )
    except ValueError:
        # FIXME: only take orig defaults when they don't break arg no default arg order :/
        return py_sig

def _fake_validation_bog(
    task_obj,
    simulated_pos_args: tuple[Any, ...],
    simulated_kwargs: dict[str, Any],
):
    from firexkit.task import FireXTask # FIXME: bad relationship between core abstractions
    task_obj : FireXTask
    bog = BagOfGoodies(
        # there is something insane in UT here where the base class isn't set.
        # I think this is due to the wrong app being used due to module-level init.
        # this means the UT is very low-fidelity.
        # getattr(task_obj, 'sig', inspect.signature(task_obj.run)),
        _merged_python_signature(task_obj),
        simulated_pos_args,
        simulated_kwargs,
        pydantic_validate=task_obj.get_pydantic_validate(),
    )
    bog.update(
        # add fake stuff that can be generated.
        {
            k: True  # we ignore falsy values when resolving????
            for k in bog.get_post_pydantic_convert_supplied_arg_names()
        }
    )
    return bog


class SignatureX(Signature):
    """
        Fake class to make intended support of monkey-patched celery.Signature
        more clear. Ideally this class would actually be used, but this is step1.
        Ideally FireX would use Celery's builtin extension mechanisms instead of
        monkey patching.
    """

    def verify_args(self) -> None:
        from firexkit.task import FireXTask # FIXME: bad relationship between core abstractions

        prev_task_return_args : Optional[dict[str, Any]] = None
        task_names_to_missing_required_arg_names : dict[str, set[str]] = {}
        chain_depth = 0
        for task_sig in [t for t in self._get_sigs()]:
            simulated_pos_args, simulated_kwargs = _simulate_chain_args_kwargs(
                prev_task_return_args,
                task_sig.args,
                task_sig.kwargs,
                chain_depth,
            )

            # I think task_sig/task_obj is bound/unbound distinction, or maybe something
            # to do with plugins, but it's not clear.
            task_obj : FireXTask = self.app.tasks[task_sig.task]
            task_bog = _fake_validation_bog(task_obj, simulated_pos_args, simulated_kwargs)
            chain_depth += 1

            unbound_required_arg_names = task_bog.get_unbound_required_arg_names()
            if unbound_required_arg_names:
                task_names_to_missing_required_arg_names[task_obj.name] = unbound_required_arg_names

            # If any of the previous keys has a dynamic return, then we can't do any validation
            if task_obj.has_dynamic_returns():
                break

            undefined_indirect = task_bog.get_args_to_indirect_value_keys()
            if undefined_indirect:
                txt = "\n".join([f"{k}: {v}" for k, v in undefined_indirect.items()])
                raise InvalidChainArgsException(
                    msg=f'Service {task_obj.name} indirectly references the following unavailable parameters: \n{txt}',
                    wrong_args=undefined_indirect,
                )

            prev_task_return_args = dict(
                task_bog.all_supplied_args()
                | {
                    rk: True # we ignore falsy values when resolving???
                    for rk in task_obj.return_keys
                }
            )

        if task_names_to_missing_required_arg_names:
            service_msgs = []
            for task_name, arg_names in task_names_to_missing_required_arg_names.items():
                service_msgs.append(f' {", ".join(arg_names)} \t required by {task_name}')
            msg = "\n".join(service_msgs)
            raise InvalidChainArgsException(
                msg=f'Missing mandatory arguments: \n{msg}',
                wrong_args=task_names_to_missing_required_arg_names,
            )

    def injectArgs(self, **kwargs):
        task_sig = self.get_first_sig()

        # Don't update kwargs in-place, because that may change the kwargs in the original object
        # even where the object was cloned in __or__ above (kwargs isn't copied and is still the same object)
        task_sig.kwargs = kwargs | task_sig.kwargs

    def get_first_sig(self) -> 'SignatureX':
        try:
            return self.tasks[0] # This might be a chain
        except AttributeError:
            return self # This might be a signature

    def _get_sigs(self) -> list['SignatureX']:
        try:
            tasks : list['SignatureX'] = self.tasks
        except AttributeError:
            return [self]
        else:
            if len(tasks) > 1:
                return [
                    t for t in tasks
                    # one of many hacks needed due to InjectArgs hack.
                    if t.name is not None
                ]
            return tasks

    def remove_inject_args(self):
        if (
            self._is_chain()
            and len( tasks := self.tasks ) > 1
        ):
            for s in tasks:
                if s.name is None:
                    logger.error(
                        f'removing {s} with name {s.name}'
                    )
            self.tasks = [
                s for s in tasks
                # one of many hacks need due to InjectArgs hack.
                if s.name is not None
            ]

    def enqueue(
        self,
        block: bool = False,
        raise_exception_on_failure: bool = True,
        caller_task: Optional[Signature]=None,
        queue: Optional[str]=None,
        priority: Optional[int]=None,
        soft_time_limit: Optional[int]=None,
    ) -> AsyncResult:

        self.remove_inject_args()
        self.verify_args()

        if queue:
            self.set_queue(queue)

        if priority:
            self.set_priority(priority)

        if soft_time_limit:
            self.set_soft_time_limit(soft_time_limit)

        result_promise = self.delay()
        if block:
            wait_on_async_results_and_maybe_raise(
                results=result_promise,
                raise_exception_on_failure=raise_exception_on_failure,
                caller_task=caller_task,
            )
        return result_promise

    def _set(self, **kwargs):
        # one of many hacks needed for InjectArgs
        if self.options is not None:
            Signature.set(self, **kwargs)

    def set_use_cache(self, use_cache: bool):
        """Set the :attr:`use_cache` execution option in every task in :attr:`sig`"""
        for s in self._get_sigs():
            s._set(use_cache=use_cache)

    def set_priority(self, priority: int):
        """Set the :attr:`priority` execution option in every task in :attr:`sig`"""
        for s in self._get_sigs():
            s._set(priority=priority)

    def set_queue(self, queue):
        """Set the :attr:`queue` execution option in every task in :attr:`sig`"""
        for s in self._get_sigs():
            s._set(queue=queue)

    def set_soft_time_limit(self, soft_time_limit):
        """Set the :attr:`soft_time_limit` execution option in every task in :attr:`sig`"""
        for s in self._get_sigs():
            s._set(soft_time_limit=soft_time_limit)

    def set_label(self, label):
        self.set(label=label)

    def get_label(self):
        try:
            if self.options:
                return self.options['label']
        except KeyError:
            pass
        return '|'.join([s.name for s in self._get_sigs()])

    def _is_chain(self) -> bool:
        return hasattr(self, 'tasks')

    def apply_async_x(self, auto_inject_reg: Optional[AutoInjectRegistry]) -> AsyncResult:
        self.remove_inject_args()
        first_sig = self.get_first_sig()
        if (
            auto_inject_reg
            and not first_sig.kwargs.get(AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY)
        ):
            first_sig.kwargs[AutoInjectRegistry.AUTO_IN_REG_ABOG_KEY] = auto_inject_reg

        if self._is_chain():
            chain_depth = 0
            for chain_sig in self._get_sigs():
                chain_sig.kwargs['chain_depth'] = chain_depth
                chain_depth += 1

        self.verify_args()

        # args & kwargs are expected to be set by prior kludges
        return self.apply_async()

#
# FIXME: use normal inheritance. Stop monkey patching.
#
Signature.injectArgs = SignatureX.injectArgs
Signature.set_priority = SignatureX.set_priority
Signature.set_use_cache = SignatureX.set_use_cache
Signature.set_queue = SignatureX.set_queue
Signature.set_soft_time_limit = SignatureX.set_soft_time_limit
Signature.set_label = SignatureX.set_label
Signature.get_label = SignatureX.get_label
Signature.enqueue = SignatureX.enqueue
Signature.verify_args = SignatureX.verify_args
Signature.get_first_sig = SignatureX.get_first_sig
Signature._get_sigs = SignatureX._get_sigs
Signature.apply_async_x = SignatureX.apply_async_x
Signature._is_chain = SignatureX._is_chain
Signature.remove_inject_args = SignatureX.remove_inject_args
Signature._set = SignatureX._set

