from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('dynd')

from odo.backends.dynd import convert, append, create, discover
from odo import into, append, convert, resource, discover
from dynd import nd
import numpy as np
import datashape

def test_create():
    ds = datashape.dshape('5 * int32')
    d = create(nd.array, dshape=ds)
    assert discover(d) == ds

def test_convert():
    ds = datashape.dshape('3 * int32')
    x = convert(nd.array, [1, 2, 3], dshape=ds)
    assert isinstance(x, nd.array)
    assert convert(list, x) == [1, 2, 3]

    for cls in [list, tuple, np.ndarray]:
        y = convert(cls, x)
        assert isinstance(y, cls)
        assert isinstance(convert(nd.array, y), nd.array)

    x = nd.array([('a', 1)], type='1 * {a: string, b: int32}')
    assert convert(list, x) == [('a', 1)]
