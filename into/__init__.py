from __future__ import absolute_import, division, print_function

from multipledispatch import halt_ordering, restart_ordering

halt_ordering() # Turn off multipledispatch ordering

from .convert import convert
from .append import append
from .create import create
from .resource import resource
from .into import into
from .drop import drop
from .chunks import chunks, Chunks
from datashape import discover, dshape

try:
     from .backends.pandas import pd
except:
    pass
try:
     from .backends.bcolz import bcolz
except:
    pass
try:
     from .backends.h5py import h5py
except:
    pass
try:
     from .backends.pytables import tables
except:
    pass
try:
     from .backends.dynd import nd
except:
    pass
try:
     from .backends import sql
except:
    pass
try:
     from .backends import mongo
except:
    pass
try:
     from .backends.csv import CSV
except:
    pass
try:
     from .backends import sql_csv
except:
    pass


restart_ordering() # Restart multipledispatch ordering and do ordering

__version__ = '0.1.0'


def test(verbose=False, junitfile=None, exit=False):
    """
    Runs the full into test suite, outputting
    the results of the tests to sys.stdout.

    This uses py.test to discover which tests to
    run, and runs tests in any 'tests' subdirectory
    within the Blaze module.

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
    sys.stdout.flush()

    # Ask pytest to do its thing
    error_code = pytest.main(args=args)
    if exit:
        return sys.exit(error_code)
    return error_code == 0
