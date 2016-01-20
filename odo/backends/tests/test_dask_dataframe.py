from __future__ import absolute_import, division, print_function

import pytest

pytest.importorskip('dask')
pytest.importorskip('pandas')

import pandas as pd
import pandas.util.testing as tm
import numpy as np
import datashape as ds

import dask.dataframe as dd

from odo import convert, discover


def test_discover():
    df = pd.DataFrame({'x': list('a'*5 + 'b'*5 + 'c'*5),
                       'y': range(15),
                       'z': list(map(float, range(15)))},
                       columns=['x', 'y', 'z'])
    df.x = df.x.astype('category')
    ddf = dd.from_pandas(df, npartitions=2)
    assert discover(ddf) == ds.var * ds.Record([('x', ds.Categorical(['a', 'b', 'c'])),
                                         ('y', ds.int64),
                                         ('z', ds.float64)])
    assert discover(ddf.x) == ds.var * ds.Categorical(['a', 'b', 'c'])


def test_convert():
    x = pd.DataFrame(np.arange(50).reshape(10, 5),
                     columns=list('abcde'))
    d = convert(dd.DataFrame, x, npartitions=2)
    assert isinstance(d, dd.DataFrame)


def test_convert_to_pandas_dataframe():
    x = pd.DataFrame(np.arange(50).reshape(10, 5),
                     columns=list('abcde'))
    d = convert(dd.DataFrame, x, npartitions=2)
    x2 = convert(pd.DataFrame, d)
    tm.assert_frame_equal(x2, x)


def test_convert_to_pandas_series():
    x = pd.DataFrame(np.arange(50).reshape(10, 5),
                     columns=list('abcde'))
    d = convert(dd.DataFrame, x, npartitions=2)
    a = convert(pd.Series, d.a)
    tm.assert_series_equal(a, x.a)
