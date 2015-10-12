from __future__ import absolute_import, division, print_function

import pytest
import sys
pytest.importorskip('dynd')

from odo import create, convert, discover
from dynd import nd
import numpy as np
import datashape


@pytest.fixture
def x():
    ds = datashape.dshape('3 * int32')
    return convert(nd.array, [1, 2, 3], dshape=ds)


def test_create():
    ds = datashape.dshape('5 * int32')
    d = create(nd.array, dshape=ds)
    assert discover(d) == ds


def test_simple_convert(x):
    assert isinstance(x, nd.array)
    assert convert(list, x) == [1, 2, 3]


@pytest.mark.parametrize(['typ', 'expected'],
                         [(list, [1, 2, 3]),
                          (tuple, (1, 2, 3)),
                          (np.ndarray, np.array([1, 2, 3]))])
def test_convert_different_types(x, typ, expected):
    y = convert(typ, x)
    assert isinstance(y, typ)
    assert isinstance(convert(nd.array, y), nd.array)
    assert all(lhs == rhs for lhs, rhs in zip(y, expected))


def test_convert_struct():
    x = nd.array([('a', 1)], type='1 * {a: string, b: int32}')
    assert convert(list, x) == [{'a': 'a', 'b': 1}]
