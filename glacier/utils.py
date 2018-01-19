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


def validate_multipart_bytes(num_bytes):
    """Amazon requires multipart uploads/downloads to be in multiples of
    1MB powers of 2 (ie: 1MB, 2MB, 4MB, 8MB, 16MB, 32MB, etc), up to 4GB"""
    error = ValueError('Part size must be a power of two and be between 1048576 and 4294967296 bytes.')
    if num_bytes not in [2**n for n in range(20,33)]:
        raise error
