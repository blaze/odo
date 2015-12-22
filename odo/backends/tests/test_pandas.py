from __future__ import absolute_import, division, print_function

from datetime import datetime

from datashape import discover, float64, dshape
from datashape.util.testing import assert_dshape_equal
from networkx import NetworkXNoPath
import numpy as np
import pandas as pd
import pytest

from odo import odo


data = [('Alice', 100), ('Bob', 200)]


def test_discover_dataframe():
    df = pd.DataFrame([('Alice', 100), ('Bob', 200)],
                      columns=['name', 'amount'])

    assert discover(df) == dshape('2 * {name: ?string, amount: int64}')


def test_discover_series():
    s = pd.Series([1, 2, 3])

    assert discover(s) == 3 * discover(s[0])


def test_floats_are_not_optional():
    df = pd.DataFrame([('Alice', 100), ('Bob', None)],
                      columns=['name', 'amount'])
    ds = discover(df)
    assert_dshape_equal(ds[1].types[1], float64)


def test_datetime_to_timestamp():
    dt = datetime(2014, 1, 1)
    ts = odo(dt, pd.Timestamp)
    assert isinstance(ts, pd.Timestamp)
    assert ts == pd.Timestamp('2014-01-01')


def test_nan_to_nat():
    assert odo(float('nan'), pd.Timestamp) is pd.NaT
    assert odo(np.nan, pd.Timestamp) is pd.NaT

    with pytest.raises(NetworkXNoPath):
        # Check that only nan can be converted.
        odo(0.5, pd.Timestamp)


def test_none_to_nat():
    assert odo(None, pd.Timestamp) is pd.NaT


def test_nat_to_nat():
    assert odo(pd.NaT, pd.Timestamp) is pd.NaT
