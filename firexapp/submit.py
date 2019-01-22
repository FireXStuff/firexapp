import argparse
from firexapp.plugins import plugin_support_parser


class SubmitBaseApp:

    def create_submit_parser(self, sub_parser):
        submit_parser = sub_parser.add_parser("submit",
                                              help="This tool invokes a fireX run and is the most common use of "
                                                   "firex. Check out our documentation for common usage patterns",
                                              parents=[plugin_support_parser],
                                              formatter_class=argparse.RawDescriptionHelpFormatter)

        submit_parser.set_defaults(func=self.run_submit)
        return submit_parser

    def run_submit(self, args, others):
        pass
