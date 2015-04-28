from __future__ import absolute_import, division, print_function

import pytest

gl = pytest.importorskip('graphlab')

from odo import discover, odo
from odo.utils import tmpfile
from datashape import dshape
import pandas as pd
import pandas.util.testing as tm
import numpy as np


@pytest.fixture
def nested():
    return gl.SFrame({'a': [{'b': 1}, {'a': 2, 'b': 3}, {'b': 3}, {'b': 4},
                            {'b': 5}, {'c': 6, 'b': 3}],
                      'b': list('abcab') + [None]})


@pytest.fixture
def tf():
    return gl.SFrame({'a': [1, 2, 3, 4, 5, 6], 'b': list('abcabc'),
                      'c': np.random.randn(6)})


@pytest.fixture
def sf():
    return gl.SFrame({'a': [1, 2, 3, 3, 1, 1, 2, 3, 4, 4, 3, 1],
                      'b': list('bacbaccaacbb'),
                      'c': np.random.rand(12)})


def test_discover_sframe(nested, tf):
    assert discover(nested) == dshape("var * {a: {a: ?int64, b: int64, c: ?int64}, b: ?string}")
    assert discover(tf) == dshape("var * {a: ?int64, b: ?string, c: ?float64}")


def test_discover_sarray(nested, tf):
    assert discover(nested['a']) == dshape("var * {a: ?int64, b: int64, c: ?int64}")
    assert discover(tf['a']) == dshape("var * ?int64")


def test_csv_to_sframe(tf):
    with tmpfile('.csv') as fn:
        with open(fn, 'wb') as f:
            odo(tf, pd.DataFrame).to_csv(f, index=False, header=True)
        result = odo(fn, gl.SFrame, has_header=True)
    tm.assert_frame_equal(odo(result, pd.DataFrame),
                          odo(tf, pd.DataFrame))
