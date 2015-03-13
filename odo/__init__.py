from __future__ import absolute_import, division, print_function

try:
    import h5py  #  h5py has precedence over pytables
except:
    pass
from multipledispatch import halt_ordering, restart_ordering

halt_ordering()  # Turn off multipledispatch ordering

from .utils import ignoring
from .convert import convert
from .append import append
from .resource import resource
from .directory import Directory
from .into import into
from .odo import odo
from .drop import drop
from .temp import Temp
from .backends.text import TextFile
from .chunks import chunks, Chunks
from datashape import discover, dshape
import numpy as np

with ignoring(ImportError):
    from .backends.sas import sas7bdat
with ignoring(ImportError):
    from .backends.pandas import pd
with ignoring(ImportError):
    from .backends.bcolz import bcolz
with ignoring(ImportError):
    from .backends.h5py import h5py
with ignoring(ImportError):
    from .backends.hdfstore import HDFStore
with ignoring(ImportError):
    from .backends.pytables import tables
with ignoring(ImportError):
    from .backends.dynd import nd
with ignoring(ImportError):
    from .backends import sql
with ignoring(ImportError):
    from .backends import mongo
with ignoring(ImportError):
    from .backends.csv import CSV
with ignoring(ImportError):
    from .backends.json import JSON, JSONLines
with ignoring(ImportError):
    from .backends.hdfs import HDFS
with ignoring(ImportError):
    from .backends.ssh import SSH
with ignoring(ImportError):
    from .backends import sql_csv
with ignoring(ImportError):
    from .backends.aws import S3
with ignoring(ImportError):
    from .backends import sql_csv
with ignoring(ImportError):
    from .backends.bokeh import ColumnDataSource
with ignoring(ImportError):
    from .backends.spark import RDD
with ignoring(ImportError):
    from .backends.sparksql import SparkDataFrame


restart_ordering()  # Restart multipledispatch ordering and do ordering

__version__ = '0.3.1'


def test(verbose=False, junitfile=None, exit=False):
    """
    Runs the full odo test suite, outputting
    the results of the tests to sys.stdout.

    This uses py.test to discover which tests to
    run, and runs tests in any 'tests' subdirectory
    within the odo module.

    Parameters
    ----------
    verbose : int, optional
        Value 0 prints very little, 1 prints a little bit,
        and 2 prints the test names while testing.
    junitfile : string, optional
        If provided, writes the test results to an junit xml
        style xml file. This is useful for running the tests
        in a CI server such as Jenkins.
    exit : bool, optional
        If True, the function will call sys.exit with an
        error code after the tests are finished.
    """
    import os
    import sys
    import pytest

    args = []

    if verbose:
        args.append('--verbose')

    # Output an xunit file if requested
    if junitfile is not None:
        args.append('--junit-xml=%s' % junitfile)

    # Add all 'tests' subdirectories to the options
    rootdir = os.path.dirname(__file__)
    for root, dirs, files in os.walk(rootdir):
        if 'tests' in dirs:
            testsdir = os.path.join(root, 'tests')
            args.append(testsdir)
            print('Test dir: %s' % testsdir[len(rootdir) + 1:])

    # Ask pytest to do its thing
    error_code = pytest.main(args=args)
    if exit:
        return sys.exit(error_code)
    return error_code == 0
