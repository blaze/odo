from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('h5py')

import datashape
import h5py
import numpy as np
import os
from collections import Iterator

from into.backends.h5py import append, create, resource, discover, convert
from contextlib import contextmanager
from into.utils import tmpfile
from into.chunks import chunks
from into import into, append, convert, resource, discover, cleanup, drop
from into.conftest import eq
from into.backends.hdf import HDFFile, HDFTable


@pytest.fixture
def new_file(tmpdir):
    return str(tmpdir / 'foo.h5')


@pytest.yield_fixture
def h5py_file(arr2):

    with tmpfile('.hdf5') as filename:
        f = h5py.File(filename)
        f.create_dataset('/data', data=arr2, chunks=True,
                         maxshape=(None,) + arr2.shape[1:])
        f.close()
        yield filename


@pytest.yield_fixture
def h5py_multi_nodes_file(arr2):

    with tmpfile('.hdf5') as filename:
        f = h5py.File(filename)
        f.create_dataset('/data', data=arr2, chunks=True,
                         maxshape=(None,) + arr2.shape[1:])
        f.create_dataset('/data2', data=arr2, chunks=True,
                         maxshape=(None,) + arr2.shape[1:])
        f.close()
        yield filename


def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


@pytest.fixture
def h5py_resource(h5py_file):
    uri = 'h5py://' + h5py_file
    return resource(uri)


def test_discover(h5py_resource, arr2):

    f = h5py_resource
    assert str(discover(arr2)) == str(discover(f['data']))
    assert str(discover(f)) == str(discover({'data': arr2}))


def test_append(h5py_resource, arr2):

    f = h5py_resource
    result = append(f['data'], arr2)
    assert eq(result[:], np.concatenate([arr2, arr2]))


def test_numpy(h5py_resource, arr2):
    f = h5py_resource
    assert eq(convert(np.ndarray, f['data']), arr2)


def test_chunks(h5py_file, arr2):

    uri = 'h5py://' + h5py_file + '::/data'
    expected = into(np.ndarray, uri)

    # this 'works', but you end up with an iterator on a closed file
    result = into(Iterator, uri)

    # the resource must remain open
    with resource(uri) as r:
        result = into(Iterator, r)
        assert np.array_equal(
            np.concatenate(list(iter(result))), expected)


def test_append_chunks(h5py_resource, arr2):

    f = h5py_resource
    append(f['data'], chunks(np.ndarray)([arr2, arr2]))
    assert len(f['data'][:]) == len(arr2) * 3


def test_create(new_file):

    ds = datashape.dshape('{x: int32, y: {z: 3 * int32}}')
    f = create(
        h5py.File, pathname=new_file, dshape='{x: int32, y: {z: 3 * int32}}')
    assert isinstance(f, h5py.File)
    assert f.filename == new_file
    assert discover(f) == ds


def test_create_partially_present_dataset(new_file):

    ds1 = datashape.dshape('{x: int32}')
    f = create(h5py.File, pathname=new_file, dshape=ds1)

    ds2 = datashape.dshape('{x: int32, y: 5 * int32}')
    f2 = create(h5py.File, pathname=new_file, dshape=ds2)

    assert f.filename == f2.filename
    assert list(f.keys()) == list(f2.keys())
    assert f['y'].dtype == 'i4'


def test_resource(new_file):

    ds = datashape.dshape('{x: int32, y: 3 * int32}')
    r = resource('h5py://' + new_file, dshape=ds)

    assert isinstance(r, HDFFile)
    assert discover(r) == ds


def test_resource_with_datapath(new_file):

    ds = datashape.dshape('3 * 4 * int32')
    r = resource('h5py://' + new_file + '::/data', dshape=ds)

    assert isinstance(r, HDFTable)
    assert discover(r) == ds
    assert r.pathname == new_file


def test_resource_with_variable_length(new_file):

    ds = datashape.dshape('var * 4 * int32')
    r = resource('h5py://' + new_file + '::/data', dshape=ds)

    assert r.shape.shape == (0, 4)


def test_copy_with_into(new_file):

    uri = 'h5py://' + new_file + '::/data'
    into(uri, [1, 2, 3])

    dset = resource(uri)
    assert dset.shape.shape == (3,)
    assert eq(dset[:], [1, 2, 3])


def test_varlen_dtypes(new_file):

    y = np.array([('Alice', 100), ('Bob', 200)],
                 dtype=[('name', 'O'), ('amount', 'i4')])

    uri = 'h5py://' + new_file + '::/data'
    into(uri, y)
    dset = resource(uri)
    assert into(list, dset) == into(list, dset)


def test_drop(h5py_multi_nodes_file):

    assert os.path.exists(h5py_multi_nodes_file)
    r = resource('h5py://' + h5py_multi_nodes_file)
    drop(r)

    assert not os.path.exists(h5py_multi_nodes_file)


def test_drop_table(h5py_multi_nodes_file):

    r = resource('h5py://' + h5py_multi_nodes_file + '::/data')
    drop(r)
    r = resource('h5py://' + h5py_multi_nodes_file)
    assert '/data' not in r


def test_contains(h5py_multi_nodes_file):

    r = resource('h5py://' + h5py_multi_nodes_file)
    assert '/data2' in r
    assert '/data' in r
    assert 'data' in r
    assert '/foo' not in r

    assert set(r.keys()) == set(['/data', '/data2'])


def test_into_return(arr2, tmpdir):

    target = str(tmpdir / 'foo.h5')
    uri = 'h5py://' + target + '::/data'

    # need a datapath
    with pytest.raises(ValueError):
        into(target, arr2)

    result = into(uri, arr2)
    assert result.dialect == 'h5py'

    result = into(uri, uri)
    assert result.dialect == 'h5py'

    result = into(np.ndarray, uri)
    np.array_equal(result, np.concatenate([arr2, arr2]))
