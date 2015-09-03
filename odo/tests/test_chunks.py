from __future__ import absolute_import, division, print_function

from datashape import dshape
from odo.chunks import *
from toolz import first


CL = chunks(list)


def test_chunks_basics():
    assert isinstance(CL, type)
    assert issubclass(CL, Chunks)


def test_chunks_isnt_consumable():
    cl = CL([[1, 2, 3], [4, 5, 6]])

    assert next(iter(cl)) == [1, 2, 3]
    assert next(iter(cl)) == [1, 2, 3]


def test_chunks_is_memoized():
    assert chunks(list) is chunks(list)


def test_callables():
    cl = CL(lambda: (list(range(3)) for i in range(3)))

    assert first(cl) == [0, 1, 2]
    assert first(cl) == [0, 1, 2]


def test_discover():
    cl = CL([[1, 2, 3], [4, 5, 6]])
    assert discover(cl).measure == discover(1).measure


def test_discover_no_consume():
    cl = CL(iter(([0, 1, 2], [3, 4, 5])))
    assert discover(cl) == discover(cl) == dshape('var * int64')
    assert tuple(cl) == ([0, 1, 2], [3, 4, 5])
    assert tuple(cl) == ()
