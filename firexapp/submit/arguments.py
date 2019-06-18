import os
import sys
import re
from firexkit.argument_conversion import ConverterRegister
from typing import Union
from firexapp.submit.console import setup_console_logging


logger = setup_console_logging(__name__)


def get_chain_args(other_args: []):
    """This function converts a flat list of --key value pairs into a dictionary"""
    chain_arguments = {}
    # Create arguments list for the chain
    it = iter(other_args)
    no_value_exception = None
    for x in it:
        if not x.startswith('-'):
            if no_value_exception:
                # the error was earlier
                raise no_value_exception
            raise ChainArgException('Error: Argument should start with a proper dash (- or --)\n%s' % x)

        try:
            value = next(it)
            if str(value).startswith("-"):
                # there might be an error. we'll find out later
                no_value_exception = ChainArgException(
                    'Error: Arguments must have an accompanying value\n%s' % x)
        except StopIteration:
            raise ChainArgException('Error: Arguments must have an accompanying value\n%s' % x)

        key = x.lstrip('-')
        if not re.match('^[A-Za-z].*', key):
            raise ChainArgException('Error: Argument should start with a letter\n%s' % key)
        chain_arguments[key] = value
    return chain_arguments


class ChainArgException(Exception):
    pass


class InputConverter:
    """
    This class uses a singleton object design to store converters which parse the cli arguments. Converter functions
    are stored into the singleton InputConverter object by adding the @register decorator to the top of each desired
    function.
    """
    _global_instance = None
    pre_load_was_run = False

    @classmethod
    def instance(cls) -> ConverterRegister:  # usd by tests only
        """Used for unit testing only"""
        if cls._global_instance is None:
            cls._global_instance = ConverterRegister()
        return cls._global_instance

    @classmethod
    def register(cls, *args):
        """
        Registers a callable object to be run during conversion. The callable should take in kwargs, and return a dict
        with any changes to the input arguments, or None if no changes are necessary.

        :Example single argument converter:

        @InputConverter.register
        @SingleArgDecorator('something')
        def convert_something(arg_value):
            arg_value = arg_value.upper()
            return arg_value

        :Optionally, dependencies can defined at registration:

        @InputConverter.register('other_converter', 'and_another_converter')
        @SingleArgDecorator('something')
        def convert_something(arg_value):
            arg_value = arg_value.upper()
            return arg_value

        Conversion occurs on two occasions, before microservices are loaded, or after. You can explicitly mark a
        converter to run pre-loading or post-loading of the ALL microservices by passing True (pre) or False (post)
        during registration. This design is used in the spirit of failing fast, providing early failure of runs before
        the bulk of microservices are imported. If bool is not provided, it will register to run pre unless loading has
        already occurred.

        @InputConverter.register('other_converter', False)
        @SingleArgDecorator('something')
        def convert_something(arg_value):
            ...
            return arg_value

        When a conversion fails the given function can simply call raise to instruct the user how to correct their
        inputs.
        """
        for arg in args:
            if not isinstance(arg, bool):
                continue
            if arg and cls.pre_load_was_run:
                raise Exception("Pre-microservice load conversion has already been run. "
                                "You can only register post load")
            preload = arg
            break
        else:
            preload = not cls.pre_load_was_run
            args = args + (preload,)

        if preload:
            for arg in args:
                if not callable(arg):
                    continue
                converter = arg

                # special handling of single argument decorator
                single_arg_decorator = getattr(converter, "single_arg_decorator", None)
                if not single_arg_decorator:
                    continue

                # need to override the append method of the single argument converters
                old_append = converter.append

                def new_append(*more_ags):
                    # special handling of first post load call
                    if cls.pre_load_was_run:
                        # re-register this converter, but in post
                        single_arg_decorator.args.clear()
                        InputConverter.register(converter)

                        # restore original behaviour
                        converter.append = old_append
                    old_append(*more_ags)
                converter.append = new_append

        return cls.instance().register(*args)

    @classmethod
    def convert(cls, pre_load=None, **kwargs) -> dict:
        """
        Activates conversion. kwargs provided are passed to any registered converter. This function should be called
        twice, and only twice. Once with initially loaded converters, and then with the secondary ones.

        :param pre_load: Used for testing. preload is defaulted to None and will auto populate
        """
        # Auto set whether this is preload, unless explicitly specified
        pre_load = not cls.pre_load_was_run if pre_load is None else pre_load
        if pre_load and cls.pre_load_was_run:
                raise Exception("Pre-microservice conversion was already run")

        cls.pre_load_was_run = True
        return cls.instance().convert(pre_task=pre_load, **kwargs)


@InputConverter.register
def convert_booleans(kwargs):
    """Converts standard true/false/none values to bools and None"""
    for key, value in kwargs.items():
        if not isinstance(value, str):
            continue
        if value.upper() == 'TRUE':
            value = True
        elif value.upper() == 'FALSE':
            value = False
        elif value.upper() == 'NONE':
            value = None
        kwargs[key] = value
    return kwargs


@InputConverter.register
def auto_load_pydev_debugging_plugin(kwargs):
    if not sys.gettrace() or sys.gettrace().__class__.__module__ == "coverage":
        return

    logger.debug("Debug detected")
    plugins = kwargs.get("plugins", "")
    if "pydev_debug_plugin.py" in plugins:
        logger.debug("Debug plugin already included")
        return

    # local and include the pydev debugging plugin
    logger.debug("Auto-including debug plugin")
    import firexapp.testing
    debugging_plugin = os.path.join(os.path.dirname(firexapp.testing.__file__), "pydev_debug_plugin.py")
    if plugins:
        debugging_plugin = "," + debugging_plugin
    return {"plugins": plugins + debugging_plugin}


_global_argument_whitelist = set()


def whitelist_arguments(argument_list: Union[str, list]):
    """
    Function for adding argument keys to the global argument whitelist. Used during validation of input arguments

    :param argument_list:List of argument keys to whitelist.
    :type argument_list: list
    """
    if type(argument_list) == str:
        argument_list = [argument_list]
    global _global_argument_whitelist
    _global_argument_whitelist |= set(argument_list)


def find_unused_arguments(chain_args: {}, ignore_list: [], all_tasks: []):
    """
    Function to detect any arguments that are not explicitly consumed by any microservice.

    :note: This should be run AFTER all microservices have been loaded.

    :param chain_args: The dictionary of chain args to check
    :type chain_args: dict
    :param ignore_list: A list of exception arguments that are acceptable. This usually includes application args.
    :type ignore_list: list
    :param all_tasks: A list of all microservices. Usually app.tasks
    :return: A dictionary of un-applicable arguments
    """
    import Levenshtein as Lev

    if len(chain_args) is 0:
        return {}, {}

    ignore_list += _global_argument_whitelist

    # remove any whitelisted
    unused_chain_args = chain_args.copy()
    for std_arg in ignore_list:
        if std_arg in unused_chain_args:
            unused_chain_args.pop(std_arg)

    # build up used chain arg list
    used_chain_args = []
    for _, task in all_tasks.items():
        used_chain_args.extend(getattr(task, "required_args", []))
        used_chain_args.extend(getattr(task, "optional_args", []))

    # Loop through used args and remove any found in unused list
    for used_arg in used_chain_args:
        if used_arg in unused_chain_args:
            unused_chain_args.pop(used_arg)

    # Loop through remaining unused chain args and build near-match dict
    close_matches = {}
    for unused_arg in unused_chain_args:
        close_match = {}
        for used_arg in used_chain_args:
            # for unused args less than 10 chars long, use distance method, otherwise use ratio method.
            if len(unused_arg) < 10:
                distance = Lev.distance(used_arg, unused_arg)
                if distance < 3:
                    if not close_match or close_match['distance'] > distance:
                        close_match['arg'] = used_arg
                        close_match['distance'] = distance
            else:
                match_ratio = Lev.ratio(used_arg, unused_arg)
                if match_ratio > 0.9:
                    if not close_match or close_match['ratio'] < match_ratio:
                        close_match['arg'] = used_arg
                        close_match['ratio'] = match_ratio
        # Store the closest match in the returned dict
        if close_match:
            close_matches[unused_arg] = close_match['arg']

    return unused_chain_args, close_matches
