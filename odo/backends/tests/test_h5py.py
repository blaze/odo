from __future__ import absolute_import, division, print_function

import pytest
h5py = pytest.importorskip('h5py')

import os
import sys

from distutils.version import LooseVersion

import h5py
from contextlib import contextmanager

from odo.backends.h5py import append, create, resource, discover, convert
from odo.backends.h5py import unicode_dtype

from odo.utils import tmpfile, ignoring
from odo.chunks import chunks
from odo import into, append, convert, discover, drop, odo
import datashape
import h5py
import numpy as np


@contextmanager
def file(x):
    with tmpfile('.hdf5') as fn:
        f = h5py.File(fn)
        data = f.create_dataset('/data', data=x, chunks=True,
                                maxshape=(None,) + x.shape[1:])

        try:
            yield fn, f, data
        finally:
            with ignoring(Exception):
                f.close()


x = np.ones((2, 3), dtype='i4')


def test_drop_group():
    with tmpfile('.hdf5') as fn:
        f = h5py.File(fn)
        try:
            f.create_dataset('/group/data', data=x, chunks=True,
                             maxshape=(None,) + x.shape[1:])
            drop(f['/group'])
            assert '/group' not in f.keys()
        finally:
            with ignoring(Exception):
                f.close()


def test_drop_dataset():
    with tmpfile('.hdf5') as fn:
        f = h5py.File(fn)
        try:
            data = f.create_dataset('/data', data=x, chunks=True,
                                    maxshape=(None,) + x.shape[1:])

            drop(data)
            assert '/data' not in f.keys()
        finally:
            with ignoring(Exception):
                f.close()


def test_drop_file():
    with file(x) as (fn, f, data):
        drop(f)
    assert not os.path.exists(fn)


def test_discover():
    with file(x) as (fn, f, dset):
        assert str(discover(dset)) == str(discover(x))
        assert str(discover(f)) == str(discover({'data': x}))


two_point_five_and_windows_py3 = \
    pytest.mark.skipif(sys.platform == 'win32' and
                       h5py.__version__ == LooseVersion('2.5.0') and
                       sys.version_info[0] == 3,
                       reason=('h5py 2.5.0 issue with varlen string types: '
                               'https://github.com/h5py/h5py/issues/593'))


@two_point_five_and_windows_py3
def test_discover_on_data_with_object_in_record_name():
    data = np.array([(u'a', 1), (u'b', 2)], dtype=[('lrg_object',
                                                    unicode_dtype),
                                                   ('an_int', 'int64')])
    with tmpfile('.hdf5') as fn:
        f = h5py.File(fn)
        try:
            f.create_dataset('/data', data=data)
        except:
            raise
        else:
            assert (discover(f['data']) ==
                    datashape.dshape('2 * {lrg_object: string, an_int: int64}'))
        finally:
            with ignoring(Exception):
                f.close()


def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_append():
    with file(x) as (fn, f, dset):
        append(dset, x)
        assert eq(dset[:], np.concatenate([x, x]))


def test_append_with_uri():
    with file(x) as (fn, f, dset):
        result = odo(dset, '%s::%s' % (fn, dset.name))
        assert eq(result[:], np.concatenate([x, x]))


def test_into_resource():
    with tmpfile('.hdf5') as fn:
        d = into(fn + '::/x', x)
        try:
            assert d.shape == x.shape
            assert eq(d[:], x[:])
        finally:
            d.file.close()


def test_numpy():
    with file(x) as (fn, f, dset):
        assert eq(convert(np.ndarray, dset), x)


def test_chunks():
    with file(x) as (fn, f, dset):
        c = convert(chunks(np.ndarray), dset)
        assert eq(convert(np.ndarray, c), x)


def test_append_chunks():
    with file(x) as (fn, f, dset):
        append(dset, chunks(np.ndarray)([x, x]))

        assert len(dset) == len(x) * 3


def test_create():
    with tmpfile('.hdf5') as fn:
        ds = datashape.dshape('{x: int32, y: {z: 3 * int32}}')
        f = create(h5py.File, dshape='{x: int32, y: {z: 3 * int32}}', path=fn)
        try:
            assert isinstance(f, h5py.File)
            assert f.filename == fn
            assert discover(f) == ds
        finally:
            f.close()


def test_create_partially_present_dataset():
    with tmpfile('.hdf5') as fn:
        ds1 = datashape.dshape('{x: int32}')
        f = create(h5py.File, dshape=ds1, path=fn)

        ds2 = datashape.dshape('{x: int32, y: 5 * int32}')
        f2 = create(h5py.File, dshape=ds2, path=fn)

        try:
            assert f.filename == f2.filename
            assert list(f.keys()) == list(f2.keys())
            assert f['y'].dtype == 'i4'
        finally:
            f.close()
            f2.close()



def test_resource():
    with tmpfile('.hdf5') as fn:
        ds = datashape.dshape('{x: int32, y: 3 * int32}')
        r = resource(fn, dshape=ds)
        r2 = resource(fn + '::/x')

        try:
            assert isinstance(r, h5py.File)
            assert discover(r) == ds
            assert isinstance(r2, h5py.Dataset)
        finally:
            r.close()
            r2.file.close()


def test_resource_with_datapath():
    with tmpfile('.hdf5') as fn:
        ds = datashape.dshape('3 * 4 * int32')
        r = resource(fn + '::/data', dshape=ds)

        try:
            assert isinstance(r, h5py.Dataset)
            assert discover(r) == ds
            assert r.file.filename == fn
            assert r.file['/data'] == r
        finally:
            r.file.close()


def test_resource_with_variable_length():
    with tmpfile('.hdf5') as fn:
        ds = datashape.dshape('var * 4 * int32')
        r = resource(fn + '::/data', dshape=ds)
        try:
            assert r.shape == (0, 4)
        finally:
            r.file.close()


@two_point_five_and_windows_py3
def test_resource_with_option_types():
    with tmpfile('.hdf5') as fn:
        ds = datashape.dshape('4 * {name: ?string, amount: ?int32}')
        r = resource(fn + '::/data', dshape=ds)
        try:
            assert r.shape == (4,)
            assert r.dtype == [('name', 'O'), ('amount', 'f4')]
        finally:
            r.file.close()


def test_resource_with_h5py_protocol():
    with tmpfile('.hdf5') as fn:
        assert isinstance(resource('h5py://' + fn), h5py.File)
    with tmpfile('.xyz') as fn:
        assert isinstance(resource('h5py://' + fn), h5py.File)


def test_copy_with_into():
    with tmpfile('.hdf5') as fn:
        dset = into(fn + '::/data', [1, 2, 3])
        try:
            assert dset.shape == (3,)
            assert eq(dset[:], [1, 2, 3])
        finally:
            dset.file.close()


@two_point_five_and_windows_py3
def test_varlen_dtypes():
    y = np.array([('Alice', 100), ('Bob', 200)],
                dtype=[('name', 'O'), ('amount', 'i4')])
    with tmpfile('.hdf5') as fn:
        dset = into(fn + '::/data', y)
        try:
            assert into(list, dset) == into(list, dset)
        finally:
            dset.file.close()


def test_resource_shape():
    with tmpfile('.hdf5') as fn:
        r = resource(fn+'::/data', dshape='10 * int')
        assert r.shape == (10,)
        r.file.close()
    with tmpfile('.hdf5') as fn:
        r = resource(fn+'::/data', dshape='10 * 10 * int')
        assert r.shape == (10, 10)
        r.file.close()
    with tmpfile('.hdf5') as fn:
        r = resource(fn+'::/data', dshape='var * 10 * int')
        assert r.shape == (0, 10)
        r.file.close()


@pytest.mark.parametrize('ext', ['h5', 'hdf5'])
def test_resource_with_hdfstore_extension_works(ext):
    fn = 'hdfstore:foo.%s' % ext
    with pytest.raises(NotImplementedError):
        odo(fn, np.recarray)
    assert not os.path.exists(fn)
