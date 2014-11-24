from __future__ import absolute_import, division, print_function

import sys
import platform

from .kdb import KQ, Credentials

# TODO: find out why "import blaze" makes import faster than not having it here
import blaze
from .compute import QTable, discover, tables, into, resource

from . import util
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

__version__ = '0.1.0'


def print_versions(file=None):
    """Print all the versions of software that kdbpy relies on."""
    print("-=" * 38, file=file)
    print("kdbpy version: %s" % __version__, file=file)
    print("Python version: %s" % sys.version, file=file)
    (sysname, nodename, release, version, machine, processor) = platform.uname()
    print("Platform: %s-%s-%s (%s)" % (sysname, release, machine, version),
          file=file)
    if sysname == "Linux":
        print("Linux dist: %s" % " ".join(platform.linux_distribution()[:-1]),
              file=file)
    if not processor:
        processor = "not recognized"
    print("Processor: %s" % processor, file=file)
    print("Byte-ordering: %s" % sys.byteorder, file=file)
    print("-=" * 38, file=file)


def test():
    import pytest
    sys.path.insert(0, '.')
    return pytest.main(args=['-r', 'sxX', '--doctest-modules',
                             '--pyargs', 'kdbpy'])
