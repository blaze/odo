from __future__ import absolute_import, division, print_function
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

__version__ = '0.0.1'

def print_versions():
    """Print all the versions of software that kdbpy relies on."""
    import sys, platform
    print("-=" * 38)
    print("kdbpy version: %s" % __version__)
    print("Python version: %s" % sys.version)
    (sysname, nodename, release, version, machine, processor) = \
        platform.uname()
    print("Platform: %s-%s-%s (%s)" % (sysname, release, machine, version))
    if sysname == "Linux":
        print("Linux dist: %s" % " ".join(platform.linux_distribution()[:-1]))
    if not processor:
        processor = "not recognized"
    print("Processor: %s" % processor)
    print("Byte-ordering: %s" % sys.byteorder)
    print("-=" * 38)

def test(verbose=False, junitfile=None, exit=False):
    """
    Runs the full kdbpy test suite, outputting
    the results of the tests to sys.stdout.

    This uses py.test to discover which tests to
    run, and runs tests in any 'tests' subdirectory
    within the kdbpyt module.

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
    # print versions (handy when reporting problems)
    print_versions()
    sys.stdout.flush()

    # Ask pytest to do its thing
    error_code = pytest.main(args=args)
    if exit:
        return sys.exit(error_code)
    return error_code == 0
