
import numpy as np
import pytest
import itertools

from into.resource import resource
from into.utils import tmpfile

import tables as tb
import h5py
from pandas.io import pytables as hdfstore

from pandas import DataFrame

df = DataFrame({ 'id' : range(5),
                 'name' : ['Alice','Bob','Charlie','Denis','Edith'],
                 'amount' : [100,-200,300,400,-500] })
arr = np.array([(1, 'Alice', 100),
                (2, 'Bob', -200),
                (3, 'Charlie', 300),
                (4, 'Denis', 400),
                (5, 'Edith', -500)],
               dtype=[('id', '<i8'), ('name', 'S7'), ('amount', '<i8')])
dshape = '{id: int, name: string[7, "ascii"], amount: float32}'

ext_list = ['.h5','.hdf5']

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
        df.to_hdf(filename,'data',format='table',data_columns=True)
        yield prefix + filename

@pytest.yield_fixture(params=combos(['','hdfstore://']))
def hdfstore_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)

@pytest.yield_fixture(params=combos(['','pytables://']))
def pytables_file(request):

    ext, prefix = request.param
    with tmpfile(ext) as filename:
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
        f = h5py.File(filename)
        f.create_dataset('/data', data=arr, chunks=True,
                         maxshape=(None,) + arr.shape[1:])
        f.close()
        yield prefix + filename

@pytest.yield_fixture(params=combos(['h5py://']))
def h5py_filename(request, tmpdir):

    ext, prefix = request.param
    yield prefix + str(tmpdir / 'foobar' + ext)

def test_hdfstore_read(hdfstore_file):

    result = resource(hdfstore_file,'/data')
    assert isinstance(result, hdfstore.AppendableFrameTable)

    result = resource(hdfstore_file + '::/data')
    assert isinstance(result, hdfstore.AppendableFrameTable)

def test_hdfstore_write(hdfstore_filename):

    # this is also the default writer
    result = resource(hdfstore_filename,'/data',dshape=dshape)
    assert isinstance(result, hdfstore.AppendableFrameTable)

def test_hdfstore_write2(hdfstore_filename):

    # this is also the default writer
    result = resource(hdfstore_filename + '::/data',dshape=dshape)
    assert isinstance(result, hdfstore.AppendableFrameTable)

def test_pytables_read(pytables_file):

    result = resource(pytables_file,'/data')
    assert isinstance(result, tb.Table)

    result = resource(pytables_file + '::/data')
    assert isinstance(result, tb.Table)

def test_pytables_write(pytables_filename):

    result = resource(pytables_filename,'/data',dshape=dshape)
    assert isinstance(result, tb.Table)

def test_pytables_write2(pytables_filename):

    result = resource(pytables_filename + '::/data',dshape=dshape)
    assert isinstance(result, tb.Table)

def test_h5py_read(h5py_file):

    import pdb; pdb.set_trace()
    #### this requires a datashape to read????? ####
    result = resource(h5py_file,'/data',dshape=dshape)
    assert isinstance(result, h5py.Dataset)

def test_h5py_write(h5py_filename):

    result = resource(h5py_filename,'/data',dshape=dshape)
    assert isinstance(result, h5py.Dataset)

def test_h5py_write2(h5py_filename):

    result = resource(h5py_filename + '::/data',dshape=dshape)
    assert isinstance(result, h5py.Dataset)
