from __future__ import absolute_import, division, print_function

from into.backends.bcolz import (append, convert, ctable, carray, resource,
                                 discover, drop)
from into.chunks import chunks
from into import append, convert, discover, into
import numpy as np
from into.utils import tmpfile
import os


def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


a = carray([1, 2, 3, 4])
x = np.array([1, 2])


def test_discover():
    assert discover(a) == discover(a[:])


def test_convert():
    assert isinstance(convert(carray, np.ones([1, 2, 3])), carray)
    b = carray([1, 2, 3])
    assert isinstance(convert(np.ndarray, b), np.ndarray)


def test_chunks():
    c = convert(chunks(np.ndarray), a, chunksize=2)
    assert isinstance(c, chunks(np.ndarray))
    assert len(list(c)) == 2
    assert eq(list(c)[1], [3, 4])

    assert eq(convert(np.ndarray, c), a[:])


def test_append_chunks():
    b = carray(x)
    append(b, chunks(np.ndarray)([x, x]))
    assert len(b) == len(x) * 3


def test_append_other():
    b = carray(x)
    append(b, convert(list, x))
    assert len(b) == 2 * len(x)


def test_resource_ctable():
    with tmpfile('.bcolz') as fn:
        r = resource(fn,
                     dshape='var * {name: string[5, "ascii"], balance: int32}')

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]


def get_expectedlen(x):
    shape, cparams, dtype, _, expectedlen, _, chunklen = x.read_meta()
    return expectedlen


def test_resource_ctable_overrides_expectedlen():
    with tmpfile('.bcolz') as fn:
        r = resource(fn,
                     dshape='100 * {name: string[5, "ascii"], balance: int32}',
                     expectedlen=200)

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]
        assert all(get_expectedlen(r[c]) == 200 for c in r.names)


def test_resource_ctable_correctly_infers_length():
    with tmpfile('.bcolz') as fn:
        r = resource(fn,
                     dshape='100 * {name: string[5, "ascii"], balance: int32}')

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]
        assert all(get_expectedlen(r[c]) == 100 for c in r.names)


def test_resource_carray():
    with tmpfile('.bcolz') as fn:
        r = resource(fn, dshape='var * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert r.shape == (0,)


def test_resource_existing_carray():
    with tmpfile('.bcolz') as fn:
        r = resource(fn, dshape='var * int32')
        append(r, [1, 2, 3])
        r.flush()
        newr = resource(fn)
        assert isinstance(newr, carray)


def test_resource_carray_overrides_expectedlen():
    with tmpfile('.bcolz') as fn:
        r = resource(fn, dshape='100 * int32', expectedlen=200)

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert r.shape == (100,)
        assert get_expectedlen(r) == 200


def test_resource_ctable_correctly_infers_length():
    with tmpfile('.bcolz') as fn:
        r = resource(fn, dshape='100 * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert get_expectedlen(r) == 100


def test_into_respects_expected_len_during_append():
    with tmpfile('.bcolz') as fn:
        b = into(fn, [1, 2, 3])
        assert get_expectedlen(b) == 3
        assert len(b) == 3


def test_resource_nd_carray():
    with tmpfile('.bcolz') as fn:
        r = resource(fn, dshape='10 * 10 * 10 * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert r.shape == (10, 10, 10)


y = np.array([('Alice', 100), ('Bob', 200)],
            dtype=[('name', 'S7'), ('amount', 'i4')])

def test_convert_numpy_to_ctable():
    b = convert(ctable, y)
    assert isinstance(b, ctable)
    assert eq(b[:], y)


def test_resource_existing_ctable():
    with tmpfile('.bcolz') as fn:
        r = into(fn, y)
        r.flush()

        r2 = resource(fn)
        assert eq(r2[:], y)


def test_drop():
    with tmpfile('.bcolz') as fn:
        r = resource(fn, dshape='var * {name: string[5, "ascii"], balance: int32}')

        assert os.path.exists(fn)
        drop(fn)
        assert not os.path.exists(fn)


def test_resource_shape():
    with tmpfile('.bcolz') as fn:
        assert resource(fn, dshape='10 * int').shape == (10,)
    with tmpfile('.bcolz') as fn:
        assert resource(fn, dshape='10 * 10 * int').shape == (10, 10)
    with tmpfile('.bcolz') as fn:
        assert resource(fn, dshape='var * 10 * int').shape == (0, 10)
