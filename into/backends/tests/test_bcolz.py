from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('bcolz')

from into.backends.bcolz import (create, append, convert, ctable, carray,
                                 resource, discover, drop)

from into.chunks import chunks
from into import append, convert, resource, discover
import numpy as np
from into.utils import tmpfile
from into.conftest import eq
import os

@pytest.fixture(scope='module')
def carr():
    return carray([1, 2, 3, 4])

def test_discover(carr):
    assert discover(carr) == discover(carr[:])


def test_convert():
    assert isinstance(convert(carray, np.ones([1, 2, 3])), carray)
    b = carray([1, 2, 3])
    assert isinstance(convert(np.ndarray, b), np.ndarray)


def test_chunks(carr):
    c = convert(chunks(np.ndarray), carr, chunksize=2)
    assert isinstance(c, chunks(np.ndarray))
    assert len(list(c)) == 2
    assert eq(list(c)[1], [3, 4])

    assert eq(convert(np.ndarray, c), carr[:])


def test_append_chunks(arr2):
    b = carray(arr2)
    append(b, chunks(np.ndarray)([arr2, arr2]))
    assert len(b) == len(arr2) * 3


def test_append_other(arr2):
    b = carray(arr2)
    append(b, convert(list, arr2))
    assert len(b) == 2 * len(arr2)


def test_resource_ctable():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn,
                     dshape='var * {name: string[5, "ascii"], balance: int32}')

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]


def get_expectedlen(x):
    shape, cparams, dtype, _, expectedlen, _, chunklen = x.read_meta()
    return expectedlen


def test_resource_ctable_overrides_expectedlen():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn,
                     dshape='100 * {name: string[5, "ascii"], balance: int32}',
                     expectedlen=200)

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]
        assert all(get_expectedlen(r[c]) == 200 for c in r.names)


def test_resource_ctable_correctly_infers_length():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn,
                     dshape='100 * {name: string[5, "ascii"], balance: int32}')

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]
        assert all(get_expectedlen(r[c]) == 100 for c in r.names)


def test_resource_carray():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='var * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'

def test_resource_existing_carray():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='var * int32')
        append(r, [1, 2, 3])
        r.flush()
        newr = resource(fn)
        assert isinstance(newr, carray)


def test_resource_carray_overrides_expectedlen():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='100 * int32', expectedlen=200)

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert get_expectedlen(r) == 200


def test_resource_ctable_correctly_infers_length():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='100 * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert get_expectedlen(r) == 100


def test_resource_nd_carray():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='10 * 10 * 10 * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert r.shape[1:] == (10, 10)


y = np.array([('Alice', 100), ('Bob', 200)],
            dtype=[('name', 'S7'), ('amount', 'i4')])

def test_convert_numpy_to_ctable(arr):
    b = convert(ctable, arr)
    assert isinstance(b, ctable)
    assert eq(b[:], arr)


def test_resource_existing_ctable(arr):
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape=discover(arr))
        append(r, arr)
        r.flush()

        r2 = resource(fn)
        assert eq(r2[:], arr)

def test_drop():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='var * {name: string[5, "ascii"], balance: int32}')

        assert os.path.exists(fn)
        drop(fn)
        assert not os.path.exists(fn)
