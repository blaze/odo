from __future__ import absolute_import, division, print_function

from into.backends.bcolz import (create, append, convert, ctable, carray,
        resource, discover)
from into.chunks import chunks
from into import into, append, convert, resource, discover
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
        os.remove(fn)
        r = resource(fn, dshape='var * {name: string[5, "ascii"], balance: int32}')

        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]


def test_resource_carray():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape='var * int32')

        assert isinstance(r, carray)
        assert r.dtype == 'i4'


y = np.array([('Alice', 100), ('Bob', 200)],
            dtype=[('name', 'S7'), ('amount', 'i4')])

def test_convert_numpy_to_ctable():
    b = convert(ctable, y)
    assert isinstance(b, ctable)
    assert eq(b[:], y)


def test_resource_existing_carray():
    with tmpfile('.bcolz') as fn:
        os.remove(fn)
        r = resource(fn, dshape=discover(y))
        append(r, y)
        r.flush()

        r2 = resource(fn)
        assert eq(r2[:], y)


