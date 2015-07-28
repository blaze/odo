from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('bcolz')

from odo.backends.bcolz import (append, convert, ctable, carray, resource,
                                 discover, drop)
from odo.chunks import chunks
from odo import append, convert, discover, into
import numpy as np
from odo.utils import tmpfile, ignoring, filetext
from contextlib import contextmanager
import shutil
import os
import uuid


@contextmanager
def tmpbcolz(*args, **kwargs):
    fn = '.%s.bcolz' % str(uuid.uuid1())
    r = resource(fn, *args, **kwargs)

    try:
        yield r
    finally:
        with ignoring(Exception):
            r.flush()
        if os.path.exists(fn):
            shutil.rmtree(fn)



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
    with tmpbcolz(dshape='var * {name: string[5, "ascii"], balance: int32}') as r:
        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]


def get_expectedlen(x):
    reader = getattr(x, '_read_meta', getattr(x, 'read_meta', None))
    assert reader is not None
    shape, cparams, dtype, _, expectedlen, _, chunklen = reader()
    return expectedlen


def test_resource_ctable_overrides_expectedlen():
    with tmpbcolz(dshape='100 * {name: string[5, "ascii"], balance: int32}',
                  expectedlen=200) as r:
        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]
        assert all(get_expectedlen(r[c]) == 200 for c in r.names)


def test_resource_ctable_correctly_infers_length():
    with tmpbcolz(dshape='100 * {name: string[5, "ascii"], balance: int32}') as r:
        assert isinstance(r, ctable)
        assert r.dtype == [('name', 'S5'), ('balance', 'i4')]
        assert all(get_expectedlen(r[c]) == 100 for c in r.names)


def test_resource_carray():
    with tmpbcolz(dshape='var * int32') as r:
        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert r.shape == (0,)


def test_resource_existing_carray():
    with tmpbcolz(dshape='var * int32') as r:
        append(r, [1, 2, 3])
        r.flush()
        newr = resource(r.rootdir)
        assert isinstance(newr, carray)


def test_resource_carray_overrides_expectedlen():
    with tmpbcolz(dshape='100 * int32', expectedlen=200) as r:
        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert r.shape == (100,)
        assert get_expectedlen(r) == 200


def test_resource_ctable_correctly_infers_length():
    with tmpbcolz(dshape='100 * int32') as r:
        assert isinstance(r, carray)
        assert r.dtype == 'i4'
        assert get_expectedlen(r) == 100


def test_into_respects_expected_len_during_append():
    with tmpfile('.bcolz') as fn:
        b = into(fn, [1, 2, 3])
        assert get_expectedlen(b) == 3
        assert len(b) == 3
        shutil.rmtree(fn)


def test_resource_nd_carray():
    with tmpbcolz(dshape='10 * 10 * 10 * int32') as r:
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

        shutil.rmtree(fn)


def test_drop():
    with tmpbcolz(dshape='var * {name: string[5, "ascii"], balance: int32}') as b:
        assert os.path.exists(b.rootdir)
        drop(b)
        assert not os.path.exists(b.rootdir)


def test_resource_shape():
    with tmpbcolz(dshape='10 * int') as b:
        assert b.shape == (10,)
    with tmpbcolz(dshape='10 * 10 * int') as b:
        assert b.shape == (10, 10)
    with tmpbcolz(dshape='var * 10 * int') as b:
        assert b.shape == (0, 10)


def test_csv_to_bcolz():
    with filetext('name,runway,takeoff,datetime_nearest_close\n'
                  'S28,28,TRUE,A\n'
                  'S16,16,TRUE,Q\n'
                  'L14,14,FALSE,I', extension='csv') as src:
        with tmpfile('bcolz') as tgt:
            bc = into(tgt, src)
            assert len(bc) == 3
