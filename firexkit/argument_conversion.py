import inspect
import os

from collections import namedtuple
from datetime import datetime
from functools import wraps
from celery.utils.log import get_task_logger
from celery.local import PromiseProxy

from firexkit.bag_of_goodies import BagOfGoodies


logger = get_task_logger(__name__)

def _get_func_file(func):
    try:
        return os.path.realpath(inspect.getfile(func))
    except TypeError:
        return None

class ConverterRegister:
    """Converters are a practical mechanism for altering the input values into microservices. They can
    also be used in upfront validation of the inputs."""
    _ConvertNode = namedtuple('ConvertNode', ['func', 'dependencies', 'file'])
    _task_instances = {}

    def __init__(self):
        """A register for argument converter functions that take in kwargs and transforms them."""
        self._pre_converters = {}
        self._post_converters = {}
        self.visit_order = None

    @classmethod
    def task_convert(cls, task_name: str, pre_task=True, **kwargs) -> dict:
        """
        Run the argument conversion for a given task.

        :param task_name: the short name of the task. If long name is given, it will be reduced to that short name
        :param pre_task: Converters can be registered to run before or after a task runs
        :param kwargs: the argument dict to be converted
        """
        task_short_name = task_name.split('.')[-1]
        if task_short_name not in cls._task_instances:
            return kwargs
        task = cls._task_instances[task_short_name]

        return task.convert(pre_task=pre_task, **kwargs)

    def convert(self, pre_task=True, verbose=True, **kwargs) -> dict:
        """ Run all registered converters
         :param pre_task: Converters can be registered to run before or after a task runs.
         """
        # Recreate the kwargs and remove any argument string
        # values which start with '@'.
        new_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, str):
                if not v.startswith(BagOfGoodies.INDIRECT_ARG_CHAR):
                    new_kwargs[k] = v
            else:
                new_kwargs[k] = v

        # we converting pre or post
        converters = self._pre_converters if pre_task else self._post_converters

        for node in self.get_visit_order(pre_task):
            if verbose:
                logger.debug("Running converter " + node)
            start = datetime.now()
            try:
                converted_dict = converters[node].func(new_kwargs)
            except Exception as e:
                logger.warning(f"Error in input converter {node}")
                raise ArgumentConversionException(f'Converter {node} failed: {e}') from e
            done = datetime.now()
            delta = (done - start).total_seconds()
            if verbose or delta >= 0.001:
                logger.debug("Took %.3f seconds to convert %s" % (delta, node))
            # handle when None is returned
            if converted_dict:
                new_kwargs.update(converted_dict)
        kwargs.update(new_kwargs)
        return kwargs

    def get_visit_order(self, pre_task=True):
        """Provide a list of all converters in order of execution, accounting for dependencies."""
        converters = self._pre_converters if pre_task else self._post_converters
        self.visit_order = []
        for converter_node in converters.values():
            self._visit_converter(converter_node=converter_node, all_converters=converters)
        return self.visit_order

    def _visit_converter(self, all_converters, converter_node, depth=0):
        if depth > len(all_converters):
            raise CircularDependencyException("A circular dependency was detected between converters")

        if converter_node.func.__name__ in self.visit_order:
            return

        for precursor in converter_node.dependencies:
            if precursor not in all_converters and precursor not in self._pre_converters:
                msg = precursor + " was not found. It is a dependency of " + converter_node.func.__name__
                raise MissingConverterDependencyError(msg)

            if converter_node.func.__name__ == precursor:
                raise CircularDependencyException(f"A converter can not be dependant on itself: {converter_node.file}#{converter_node.func.__name__} ")

            if precursor not in self.visit_order and precursor in all_converters:
                self._visit_converter(all_converters=all_converters,
                                      converter_node=all_converters[precursor],
                                      depth=depth+1)
        self.visit_order.append(converter_node.func.__name__)

    @classmethod
    def register_for_task(cls, task: PromiseProxy, pre_task=True, *args):
        """ Register a converter function for a given task.

        :param task: A microservice signature against which to register the task
        :param pre_task: At which point should this converter be called? True is pre (before task), \
                 False is post. (after task)
        :param args: A collection of optional arguments, the function of which is based on it's type:

        *  **callable** (only once): A function that will be called to convert arguments
        *  **str**: Dependencies. Any dependency of the current converter on the one in the string.

        """
        task_short_name = task.name.split('.')[-1]
        if task_short_name not in cls._task_instances:
            cls._task_instances[task_short_name] = ConverterRegister()
        task_registry = cls._task_instances[task_short_name]
        return task_registry.register(pre_task, *args)

    def register(self, *args):
        """ Register a converter function.

        :param args: A collection of optional arguments, the function of which is based on it's type:

        * **callable** (only once): A function that will be called to convert arguments
        * **boolean** (only once): At which point should this converter be called? True is pre (before task), \
                False is post. (after task)
        * **str**: Dependencies. Any dependency of the current converter on the one in the string.

        """

        if len(args) == 0:
            raise ConverterRegistrationException("Registration requires at least one argument")

        func = None
        dependencies = []
        run_pre_task = None

        for arg in args:
            if callable(arg):
                func = arg
            elif isinstance(arg, str):
                dependencies.append(arg)
            elif isinstance(arg, bool):
                run_pre_task = arg
            else:
                raise ConverterRegistrationException(
                    "Converter incorrectly registered. Type %s not recognised" % str(type(arg)))

        return self._sub_register(func=func, dependencies=dependencies, run_pre_task=run_pre_task)

    def _sub_register(self, func, dependencies: list[str], run_pre_task):

        if run_pre_task is False:
            converters = self._post_converters
        else:
            converters = self._pre_converters

        if func:
            # this is the case where decorator is used WITHOUT parenthesis
            # @InputConverter.register
            self._check_not_registered(func, converters)
            converters[func.__name__] = self._ConvertNode(
                func=func,
                dependencies=[],
                file=_get_func_file(func),
            )
            return func

        # this is the case where decorator is used WITH parenthesis
        # @ConverterRegister.register(...)
        def _wrapped_register(fn):
            if not callable(fn):
                raise ConverterRegistrationException("A converter must be callable")
            self._check_not_registered(fn, converters)
            converters[fn.__name__] = self._ConvertNode(
                func=fn,
                dependencies=dependencies,
                file=_get_func_file(fn),
            )
            return fn
        return _wrapped_register

    @staticmethod
    def _check_not_registered(func, converters):
        if callable(func):
            func_name = func.__name__
        else:
            func_name = func

        existing_conv = converters.get(func_name)
        if existing_conv:
            func_file = _get_func_file(func)
            if func_file != existing_conv.file:
                raise NameDuplicationException(
                    f"Converter {func_name} from {func_file} is already registered from {existing_conv.file}. Please define a unique name"
                )

    @classmethod
    def get_register(cls, task_name):
        task_short_name = task_name.split('.')[-1]
        return cls._task_instances.get(task_short_name)

    @classmethod
    def list_converters(cls, task_name, pre_task=True):
        reg = cls.get_register(task_name)
        if not reg:
            return []
        return reg.get_visit_order(pre_task=pre_task)


class SingleArgDecorator(object):
    """
    Decorator to simplify a common use case for argument converters, in which a single argument in the
    bag of goodies needs to be validated or converted. Converter is only called if the argument is in
    kwargs.

    :Example:

    @ConverterRegister.ConverterRegister(BirthdayCake)
    @SingleArgDecorator("message"):
    def yell_loud(arg_value):
        return arg_value.upper()
    """

    def __init__(self, *args):
        """
        :param args: A lists of argument names for which this converter applies
        """
        if not args:
            raise ConverterRegistrationException("SingleArgDecorator requires at least one argument name")
        for arg in args:
            if type(arg) is not str:
                raise ConverterRegistrationException("SingleArgDecorator takes strings as inputs")

        self.args = list(args)

    def __call__(self, fn):
        @wraps(fn)
        def validator_decorator(args):
            ret = {}
            for k in self.args:
                if k in args:
                    orig_value = args[k]
                    # we don't validate ref values (i.e. @something)
                    if hasattr(orig_value, 'startswith') and orig_value.startswith(BagOfGoodies.INDIRECT_ARG_CHAR):
                        ret[k] = orig_value
                    else:
                        try:
                            v = fn(args[k])
                        except Exception as e:
                            logger.debug('The original exception thrown by the converter is:', exc_info=True)
                            raise ArgumentConversionException(k + ": " + str(e)) from None
                        if v != orig_value:
                            logger.debug("Argument %s was converted from %s to %s" % (k, str(orig_value), str(v)))
                        ret[k] = v
            return ret

        # An append method is a nice addition to allow other modules to add extra args for conversion
        def append(*args):
            self.args.extend(args)
        validator_decorator.append = append
        validator_decorator.single_arg_decorator = self

        return validator_decorator


class ArgumentConversionException(Exception):
    """An exception occurred while executing a converter"""
    pass


class ConverterRegistrationException(Exception):
    """A coding error in the registration of the converter"""
    pass


class MissingConverterDependencyError(ConverterRegistrationException):
    """A converter was registered with a dependency that does not exist."""
    pass


class CircularDependencyException(ConverterRegistrationException):
    """A converter was registered with a dependency that is itself directly or indirectly dependent on it."""
    pass


class NameDuplicationException(ConverterRegistrationException):
    """A converter was registered with the same name as another converter. This creates conflicts during dependency
    check, and is not allow"""
    pass
