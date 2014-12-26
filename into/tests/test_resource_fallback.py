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
from into.resource import resource
from into.utils import tmpfile

import h5py
import tables as tb
from pandas.io import pytables as hdfstore

from pandas import DataFrame
IS_PY3 = sys.version_info[0] >= 3

df = DataFrame({ 'id' : range(5),
                 'name' : ['Alice','Bob','Charlie','Denis','Edith'],
                 'amount' : [100,-200,300,400,-500] })
arr = np.array([(1, 'Alice', 100),
                (2, 'Bob', -200),
                (3, 'Charlie', 300),
                (4, 'Denis', 400),
                (5, 'Edith', -500)],
               dtype=[('id', '<i8'), ('name', 'S7'), ('amount', '<i8')])
dshape = discover(arr)

ext_list = ['.h5','.hdf5']

@contextmanager
def ensure_resource_clean(*args, **kwargs):
    result = resource(*args, **kwargs)
    yield result

    # hdfstore
    try:
        result.parent.close()
    except AttributeError:
        pass

    # pytables
    try:
        result._v_file.close()
    except AttributeError:
        pass

    # h5py
    try:
        result.close()
    except AttributeError:
        pass


def combos(prefixes):
    """ return all combinations of the ext and the prefixes list """
    list1 = ext_list
    list2 = prefixes
    l = []
    for x in itertools.permutations(list1,len(list2)):
        l.extend(list(zip(x,list2)))
    return l

@pytest.yield_fixture(params=combos(['','hdfstore://']))
def hdfstore_file(request):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
        df.to_hdf(filename,'data',mode='w',format='table',data_columns=True)
        yield prefix + filename

@pytest.yield_fixture(params=combos(['','hdfstore://']))
def hdfstore_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)

@pytest.yield_fixture(params=combos(['','pytables://']))
def pytables_file(request):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
        os.remove(filename)
        f = tb.open_file(filename, mode='w')
        d = f.create_table('/', 'data',  arr)
        d.close()
        f.close()
        yield prefix + filename

@pytest.yield_fixture(params=combos(['pytables://']))
def pytables_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)

@pytest.yield_fixture(params=combos(['','h5py://']))
def h5py_file(request):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
        os.remove(filename)
        f = h5py.File(filename,mode='w')
        f.create_dataset('/data', data=arr, chunks=True,
                         maxshape=(None,) + arr.shape[1:])
        f.close()
        yield prefix + filename

@pytest.yield_fixture(params=combos(['h5py://']))
def h5py_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)

def test_hdfstore_write(hdfstore_filename):

    # this is also the default writer
    with ensure_resource_clean(hdfstore_filename,'/data',dshape=dshape) as result:
        assert isinstance(result, hdfstore.AppendableFrameTable)

def test_hdfstore_write2(hdfstore_filename):

    # this is also the default writer
    with ensure_resource_clean(hdfstore_filename + '::/data',dshape=dshape) as result:
        assert isinstance(result, hdfstore.AppendableFrameTable)

def test_hdfstore_read(hdfstore_file):

    with ensure_resource_clean(hdfstore_file,'/data') as result:
        assert isinstance(result, hdfstore.AppendableFrameTable)

def test_hdfstore_read2(hdfstore_file):

    with ensure_resource_clean(hdfstore_file + '::/data') as result:
        assert isinstance(result, hdfstore.AppendableFrameTable)

# These seems to cause segfaults if run in concert with the PyTables tests in the same process
# http://stackoverflow.com/questions/7450881/python-segmentation-fault-when-closing-quitting

#@pytest.mark.skipif(not IS_PY3, reason="hp5y fail under < 3")
#def test_h5py_write(h5py_filename):

#    with ensure_resource_clean(h5py_filename,'/data',dshape=dshape) as result:
#        assert isinstance(result, h5py.Dataset)

#@pytest.mark.skipif(not IS_PY3, reason="hp5y fail under < 3")
#def test_h5py_write2(h5py_filename):

#    with ensure_resource_clean(h5py_filename + '::/data',dshape=dshape) as result:
#        assert isinstance(result, h5py.Dataset)

#@pytest.mark.skipif(not IS_PY3, reason="hp5y fail under < 3")
#def test_h5py_read(h5py_file):

    with ensure_resource_clean(h5py_file,'/data',dshape=dshape) as result:
        assert isinstance(result, h5py.Dataset)

def test_pytables_write(pytables_filename):

    with ensure_resource_clean(pytables_filename,'/data',dshape=dshape) as result:
        assert isinstance(result, tb.Table)

def test_pytables_write2(pytables_filename):

    with ensure_resource_clean(pytables_filename + '::/data',dshape=dshape) as result:
        assert isinstance(result, tb.Table)

def test_pytables_read(pytables_file):

    with ensure_resource_clean(pytables_file, '/data') as result:
        assert isinstance(result, tb.Table)

def test_pytables_read2(pytables_file):

    with ensure_resource_clean(pytables_file + '::/data') as result:
        assert isinstance(result, tb.Table)
