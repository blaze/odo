"""

test the use of uri's in resource that need a hierarchical dismbiguation
based on priority and the raising of NotImplementedError by matches
that do not wish to be a resource for that particular uri

IOW. a higher-priority uri matches (e.g. .h5), but the data is
actually PyTables, so HDFStore 'passes' on the processing. PyTables
is next in the chain so it will then process.

Note that a uri can be more fully-disambiguated via a head element
The default is hdfstore (this only matters for writing), reading
is unambiguous.

e.g.

....../file.h5 -> hdfstore
hdfstore://....../file.hdf5|h5 -> hdfstore
pytables://....../file.hdf5|h5 -> pytables
h5py://........../file.hdf5|h5 -> h5py

"""


import sys
import os
import numpy as np
import pytest
import itertools
from contextlib import contextmanager

from datashape import discover
from into.cleanup import cleanup
from into.resource import resource
from into.utils import tmpfile

import h5py
import tables as tb
from pandas.io import pytables as hdfstore
from into.backends.hdf import HDFFile, HDFTable

from pandas import DataFrame
IS_PY3 = sys.version_info[0] >= 3


def generate_uri_combos(prefixes):
    """
    return all combinations of the ext and the prefixes list

    return a dict suitable for use in a pytest fixture

    """
    list1 = ['.h5', '.hdf5']
    list2 = prefixes
    l = []
    for x in itertools.permutations(list1, len(list2)):
        l.extend(list(zip(x, list2)))

    return {'params': l, 'ids': l}


@pytest.yield_fixture(**generate_uri_combos(['', 'hdfstore://']))
def hdfstore_file(request, df):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
        df.to_hdf(
            filename, 'data', mode='w', format='table', data_columns=True)
        yield prefix + filename


@pytest.yield_fixture(**generate_uri_combos(['', 'hdfstore://']))
def hdfstore_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)


@pytest.yield_fixture(**generate_uri_combos(['', 'pytables://']))
def pytables_file(request, arr):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
        os.remove(filename)
        f = tb.open_file(filename, mode='w')
        d = f.create_table('/', 'data',  arr)
        d.close()
        f.close()
        yield prefix + filename


@pytest.yield_fixture(**generate_uri_combos(['pytables://']))
def pytables_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)


@pytest.yield_fixture(**generate_uri_combos(['', 'h5py://']))
def h5py_file(request, arr):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
        os.remove(filename)
        f = h5py.File(filename, mode='w')
        f.create_dataset('/data', data=arr, chunks=True,
                         maxshape=(None,) + arr.shape[1:])
        f.close()
        yield prefix + filename


@pytest.yield_fixture(**generate_uri_combos(['h5py://']))
def h5py_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)


def test_hdfstore_write(hdfstore_filename, arr_dshape):

    # this is also the default writer
    result = resource(hdfstore_filename, '/data', dshape=arr_dshape)
    assert result.dialect == 'HDFStore'
    assert isinstance(result, HDFTable)


def test_hdfstore_write2(hdfstore_filename, arr_dshape):

    # this is also the default writer
    result = resource(hdfstore_filename + '::/data', dshape=arr_dshape)
    assert result.dialect == 'HDFStore'
    assert isinstance(result, HDFTable)


def test_hdfstore_read(hdfstore_file):

    result = resource(hdfstore_file, '/data')
    assert result.dialect == 'HDFStore'
    assert isinstance(result, HDFTable)


def test_hdfstore_read2(hdfstore_file):

    result = resource(hdfstore_file + '::/data')
    assert result.dialect == 'HDFStore'
    assert isinstance(result, HDFTable)

# def test_h5py_write(h5py_filename, arr_dshape):

#    result = resource(h5py_filename, '/data', dshape=arr_dshape)
#    assert result.dialect == 'h5py'
#    assert isinstance(result, HDFTable)

# def test_h5py_write2(h5py_filename, arr_dshape):

#    result = resource(h5py_filename + '::/data', dshape=arr_dshape)
#    assert result.dialect == 'h5py'
#    assert isinstance(result, HDFTable)


def test_h5py_read(h5py_file):

    result = resource(h5py_file, '/data')
    assert result.dialect == 'h5py'
    assert isinstance(result, HDFTable)


def test_pytables_write(pytables_filename, arr_dshape):

    result = resource(pytables_filename, '/data', dshape=arr_dshape)
    assert result.dialect == 'PyTables'
    assert isinstance(result, HDFTable)


def test_pytables_write2(pytables_filename, arr_dshape):

    result = resource(pytables_filename + '::/data', dshape=arr_dshape)
    assert result.dialect == 'PyTables'
    assert isinstance(result, HDFTable)


def test_pytables_read(pytables_file):

    result = resource(pytables_file, '/data')
    assert result.dialect == 'PyTables'
    assert isinstance(result, HDFTable)


def test_pytables_read2(pytables_file):

    result = resource(pytables_file + '::/data')
    assert result.dialect == 'PyTables'
    assert isinstance(result, HDFTable)
