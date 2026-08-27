# LOAD NO SLOW MODULES HERE!
# This is to keep loading the launcher module fast.
import os

from firexapp.submit.uid import Uid


def get_blaze_dir(logs_dir, instance_name=None):
    if instance_name is None:
        instance_name = 'blaze'
    return os.path.join(logs_dir, Uid.debug_dirname, instance_name)
