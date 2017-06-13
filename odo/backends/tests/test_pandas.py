from __future__ import absolute_import, division, print_function

from collections import OrderedDict
from datetime import datetime, timedelta
from distutils.version import LooseVersion

from datashape import (
    Categorical,
    DateTime,
    Option,
    Record,
    discover,
    dshape,
    float64,
    int64,
    coretypes as ct,
)
from datashape.util.testing import assert_dshape_equal
from networkx import NetworkXNoPath
import numpy as np
import pandas as pd
import pytest

from odo import odo


requires_datetimetz = pytest.mark.skipif(
    LooseVersion(pd.__version__) < LooseVersion('0.17'),
    reason="Pandas before DatetimeTZ",
)


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
    assert odo(float('nan'), pd.Timedelta) is pd.NaT
    assert odo(np.nan, pd.Timestamp) is pd.NaT
    assert odo(np.nan, pd.Timedelta) is pd.NaT

    with pytest.raises(NetworkXNoPath):
        # Check that only nan can be converted.
        odo(0.5, pd.Timestamp)

    with pytest.raises(NetworkXNoPath):
        # Check that only nan can be converted.
        odo(0.5, pd.Timedelta)


def test_none_to_nat():
    assert odo(None, pd.Timestamp) is pd.NaT
    assert odo(None, pd.Timedelta) is pd.NaT


def test_nat_to_nat():
    assert odo(pd.NaT, pd.Timestamp) is pd.NaT
    assert odo(pd.NaT, pd.Timedelta) is pd.NaT


def test_timedelta_to_pandas():
    assert odo(timedelta(days=1), pd.Timedelta) == pd.Timedelta(days=1)
    assert odo(timedelta(hours=1), pd.Timedelta) == pd.Timedelta(hours=1)
    assert odo(timedelta(seconds=1), pd.Timedelta) == pd.Timedelta(seconds=1)


def test_categorical_pandas():
    df = pd.DataFrame({'x': list('a'*5 + 'b'*5 + 'c'*5),
                       'y': np.arange(15, dtype=np.int64)},
		      columns=['x', 'y'])
    df.x = df.x.astype('category')
    assert_dshape_equal(discover(df), 15 * Record([('x',
                        Categorical(['a', 'b', 'c'])), ('y', int64)]))
    assert_dshape_equal(discover(df.x), 15 * Categorical(['a', 'b', 'c']))


@requires_datetimetz
def test_datetimetz_pandas():
    df = pd.DataFrame(
        OrderedDict([
            ('naive', pd.date_range('2014', periods=5)),
            ('Europe/Moscow', pd.date_range('2014', periods=5, tz='Europe/Moscow')),
            ('UTC', pd.date_range('2014', periods=5, tz='UTC')),
            ('US/Eastern', pd.date_range('2014', periods=5, tz='US/Eastern')),
        ])
    )

    assert_dshape_equal(
        discover(df),
        5 * Record[
            'naive': Option(DateTime(tz=None)),
            'Europe/Moscow': Option(DateTime(tz='Europe/Moscow')),
            'UTC': Option(DateTime(tz='UTC')),
            'US/Eastern': Option(DateTime(tz='US/Eastern')),
        ]
    )

    assert_dshape_equal(discover(df.naive), 5 * Option(DateTime(tz=None)))
    for tz in ('Europe/Moscow', 'UTC', 'US/Eastern'):
        assert_dshape_equal(
            discover(df[tz]),
            5 * Option(DateTime(tz=tz))
        )


@pytest.mark.parametrize('dtype', ('int64', 'float64'))
def test_numeric_index(dtype):
    ix = pd.Index([1, 2, 3], dtype=dtype)
    actual = discover(ix)
    expected = 3 * getattr(ct, dtype)

    assert_dshape_equal(actual, expected)


@pytest.mark.parametrize(
    'tz', (
        None,
        requires_datetimetz('US/Eastern'),
        requires_datetimetz('UTC'),
        requires_datetimetz('Europe/Moscow'),
    ),
)
def test_datetime_index(tz):
    ix = pd.DatetimeIndex(['2014-01-01', '2014-01-02', '2014-01-03'], tz=tz)
    actual = discover(ix)
    expected = 3 * Option(DateTime(tz=tz))

    assert_dshape_equal(actual, expected)
