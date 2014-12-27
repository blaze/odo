from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('h5py')

from into.backends.h5py import append, create, resource, discover, convert
from contextlib import contextmanager
from into.utils import tmpfile
from into.chunks import chunks
from into import into, append, convert, resource, discover, cleanup
from into.conftest import eq
import datashape
import h5py
import numpy as np
import os

@contextmanager
def ensure_clean_store(data, ext=None):

    if ext is None:
        ext = '.hdf5'

    with tmpfile(ext) as filename:
        f = h5py.File(filename)
        data = f.create_dataset('/data', data=data, chunks=True,
                                maxshape=(None,) + data.shape[1:])

        try:
            yield f
        finally:
            cleanup(f)

def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c

def test_discover(arr2):
    with ensure_clean_store(arr2) as f:
        assert str(discover(arr2)) == str(discover(f['data']))
        assert str(discover(f)) == str(discover({'data': arr2}))

def test_append(arr2):
    with ensure_clean_store(arr2) as f:
        append(f['data'], arr2)
        assert eq(f['data'][:], np.concatenate([arr2 ,arr2]))


def test_numpy(arr2):
    with ensure_clean_store(arr2) as f:
        assert eq(convert(np.ndarray, f['data']), arr2)


def test_chunks(arr2):
    with ensure_clean_store(arr2) as f:
        c = convert(chunks(np.ndarray), f['data'])
        assert eq(convert(np.ndarray, c), arr2)


def test_append_chunks(arr2):
    with ensure_clean_store(arr2) as f:
        append(f['data'], chunks(np.ndarray)([arr2, arr2]))
        assert len(f['data']) == len(arr2) * 3


def test_create():
    with tmpfile('.hdf5') as fn:
        ds = datashape.dshape('{x: int32, y: {z: 3 * int32}}')
        f = create(h5py.File, dshape='{x: int32, y: {z: 3 * int32}}', path=fn)
        assert isinstance(f, h5py.File)
        assert f.filename == fn
        assert discover(f) == ds
        f.close()

def test_create_partially_present_dataset():
    with tmpfile('.hdf5') as fn:
        os.remove(fn)
        ds1 = datashape.dshape('{x: int32}')
        f = create(h5py.File, dshape=ds1, path=fn)

        ds2 = datashape.dshape('{x: int32, y: 5 * int32}')
        f2 = create(h5py.File, dshape=ds2, path=fn)

        assert f.filename == f2.filename
        assert list(f.keys()) == list(f2.keys())
        assert f['y'].dtype == 'i4'
        f.close()
        f2.close()

def test_resource():
    with tmpfile('.hdf5') as fn:
        os.remove(fn)
        ds = datashape.dshape('{x: int32, y: 3 * int32}')
        r = resource('h5py://' + fn, dshape=ds)

        assert isinstance(r, h5py.File)
        assert discover(r) == ds
        cleanup(r)

def test_resource_with_datapath():
    with tmpfile('.hdf5') as fn:
        os.remove(fn)
        ds = datashape.dshape('3 * 4 * int32')
        r = resource('h5py://' + fn + '::/data', dshape=ds)

        assert isinstance(r, h5py.Dataset)
        assert discover(r) == ds
        assert r.file.filename == fn
        assert r.file['/data'] == r
        cleanup(r)

def test_resource_with_variable_length():
    with tmpfile('.hdf5') as fn:
        os.remove(fn)
        ds = datashape.dshape('var * 4 * int32')
        r = resource('h5py://' + fn + '::/data', dshape=ds)

        assert r.shape == (0, 4)
        cleanup(r)

def test_copy_with_into():
    with tmpfile('.hdf5') as fn:
        uri = 'h5py://' + fn + '::/data'
        into(uri, [1, 2, 3])

        dset = resource(uri)
        assert dset.shape == (3,)
        assert eq(dset[:], [1, 2, 3])
        cleanup(dset)

def test_varlen_dtypes():
    y = np.array([('Alice', 100), ('Bob', 200)],
                dtype=[('name', 'O'), ('amount', 'i4')])
    with tmpfile('.hdf5') as fn:
        uri = 'h5py://' + fn + '::/data'
        into(uri, y)
        dset = resource(uri)
        assert into(list, dset) == into(list, dset)
        cleanup(dset)
