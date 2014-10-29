from __future__ import absolute_import, division, print_function

import sys
import platform

# TODO: find out why "import blaze" makes import faster than not having it here
import blaze
from .compute import *

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

__version__ = '0.0.2'


def print_versions():
    """Print all the versions of software that kdbpy relies on."""
    print("-=" * 38)
    print("kdbpy version: %s" % __version__)
    print("Python version: %s" % sys.version)
    (sysname, nodename, release, version, machine, processor) = platform.uname()
    print("Platform: %s-%s-%s (%s)" % (sysname, release, machine, version))
    if sysname == "Linux":
        print("Linux dist: %s" % " ".join(platform.linux_distribution()[:-1]))
    if not processor:
        processor = "not recognized"
    print("Processor: %s" % processor)
    print("Byte-ordering: %s" % sys.byteorder)
    print("-=" * 38)
