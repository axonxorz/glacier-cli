from __future__ import print_function
from __future__ import unicode_literals

import os
import errno


def mkdir_p(path):
    """Create path if it doesn't exist already"""
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
