import json
import os
import re
import datetime
import pytz
import argparse
import tempfile
from getpass import getuser
from firexapp.plugins import plugin_support_parser


class SubmitBaseApp:
    def __init__(self):
        self.parser = None

    def create_submit_parser(self, sub_parser):
        if self.parser:
            return self.parser
        submit_parser = sub_parser.add_parser("submit",
                                              help="This tool invokes a fireX run and is the most common use of "
                                                   "firex. Check out our documentation for common usage patterns",
                                              parents=[plugin_support_parser],
                                              formatter_class=argparse.RawDescriptionHelpFormatter)

        submit_parser.set_defaults(func=self.run_submit)
        self.parser = submit_parser
        return self.parser

    def run_submit(self, args, others):
        chain_args = self.process_other_chain_args(args, others)

        uid = Uid()
        chain_args['uid'] = uid
        print('Logs: %s' % uid.logs_dir)  # todo: use logger

        # Create an env file
        with open(os.path.join(uid.logs_dir, "environ.json"), 'w') as f:
            json.dump(dict(os.environ), fp=f, skipkeys=True, sort_keys=True, indent=4)

    def process_other_chain_args(self, args, other_args)-> {}:
        chain_args = {}
        try:
            chain_args = get_chain_args(other_args)
        except ChainArgException as e:
            self.parser.exit(-1, str(e) + '\nAborting...')

        return chain_args


class Uid(object):
    def __init__(self, identifier=None):
        self.timestamp = datetime.datetime.now(tz=pytz.utc)
        self.user = getuser()
        if identifier:
            self.identifier = identifier
        else:
            self.identifier = 'FireX-%s-%s-%s' % (self.user, self.timestamp.strftime("%y%m%d-%H%M%S"), os.getpid())
        self._logs_dir = None

    @property
    def base_logging_dir(self):
        return tempfile.gettempdir()

    @property
    def logs_dir(self):
        if not self._logs_dir:
            self._logs_dir = self.create_logs_dir()
        return self._logs_dir

    def create_logs_dir(self):
        path = os.path.join(self.base_logging_dir, self.identifier)
        os.makedirs(path, 0o766)
        return path

    def __str__(self):
        return self.identifier

    def __repr__(self):
        return self.identifier

    def __eq__(self, other):
        return str(other) == self.identifier


def get_chain_args(other_args):
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
