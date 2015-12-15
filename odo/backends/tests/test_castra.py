from castra import Castra
from datashape import dshape
from datashape.util.testing import assert_dshape_equal
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from odo import odo, discover, drop, resource
from odo.compatibility import TemporaryDirectory


def test_pandas_roundtrip():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [0.1, 0.2, 0.3]})
    c = odo(df, Castra)
    assert_frame_equal(c[:], df)
    assert_frame_equal(odo(c, pd.DataFrame), df)


def test_discover_castra():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [0.1, 0.2, 0.3]})
    c = odo(df, Castra)
    assert_dshape_equal(
        discover(c),
        dshape('var * {a: int64, b: float64}'),
    )


def test_append_castra():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [0.1, 0.2, 0.3]})
    c = odo(df, Castra)
    ef = pd.DataFrame({'a': [4, 5, 6], 'b': [0.4, 0.5, 0.6]})
    odo(ef, c)

    assert_frame_equal(
        odo(c, pd.DataFrame),
        pd.concat((df, ef), ignore_index=True),
    )


def test_drop():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [0.1, 0.2, 0.3]})
    c = odo(df, Castra)
    drop(c)
    with pytest.raises(IOError):  # py2 compat name
        c[:]


def test_resource():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [0.1, 0.2, 0.3]})
    with TemporaryDirectory(suffix='.castra') as d:
        odo(df, Castra, path=d)

        c = resource(d)
        assert_frame_equal(c[:], df)
