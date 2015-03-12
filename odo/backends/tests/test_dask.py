from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('dask')

from odo.backends.dask import append, merge
from dask.array.core import insert_to_ooc, Array
from dask.bag.core import Bag
from dask import core
from odo import chunks, convert, into, TextFile
from odo.utils import tmpfile, filetexts
import numpy as np

####################
# dask.array tests #
####################

def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_convert():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))

    assert isinstance(d, Array)


def test_convert_to_numpy_array():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))
    x2 = convert(np.ndarray, d)

    assert eq(x, x2)


def test_append_to_array():
    bcolz = pytest.importorskip('bcolz')
    x = np.arange(600).reshape((20, 30))
    a = into(Array, x, blockshape=(4, 5))
    b = bcolz.zeros(shape=(0, 30), dtype=x.dtype)

    append(b, a)
    assert eq(b[:], x)

    with tmpfile('hdf5') as fn:
        h = into(fn+'::/data', a)
        assert eq(h[:], x)
        h.file.close()


def test_into_inplace():
    bcolz = pytest.importorskip('bcolz')
    x = np.arange(600).reshape((20, 30))
    a = into(Array, x, blockshape=(4, 5))
    b = bcolz.zeros(shape=(20, 30), dtype=x.dtype)

    append(b, a, inplace=True)
    assert eq(b[:], x)


def test_insert_to_ooc():
    x = np.arange(600).reshape((20, 30))
    y = np.empty(shape=x.shape, dtype=x.dtype)
    a = convert(Array, x, blockshape=(4, 5))

    dsk = insert_to_ooc(y, a)
    core.get(merge(dsk, a.dask), list(dsk.keys()))

    assert eq(y, x)


def test__array__():
    x = np.arange(600).reshape((20, 30))
    d = convert(Array, x, blockshape=(4, 5))

    assert eq(x, np.array(d))

##################
# dask.bag tests #
##################


def inc(x):
    return x + 1

dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

L = list(range(5)) * 3

b = Bag(dsk, 'x', 3)

def test_convert_bag_to_list():
    assert convert(list, b) == L

def test_convert_logfiles_to_bag():
    with filetexts({'a1.log': 'Hello\nWorld', 'a2.log': 'Hola\nMundo'}) as fns:
        logs = chunks(TextFile)(list(map(TextFile, fns)))
        b = convert(Bag, logs)
        assert isinstance(b, Bag)
        assert 'a1.log' in str(b.dask.values())
        assert convert(list, b) == convert(list, logs)


def test_sequence():
    b = into(Bag, [1, 2, 3])
    assert set(b.map(inc)) == set([2, 3, 4])
