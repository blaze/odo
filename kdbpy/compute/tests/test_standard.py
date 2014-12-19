from __future__ import print_function

import pytest

import pandas as pd
import pandas.util.testing as tm

from into import into

import blaze as bz
from blaze import Data, by, compute
from blaze.expr import Field

from kdbpy.compute.qtable import is_standard


def wavg(x, y):
    return (x * y).sum() / x.sum()


@pytest.fixture(scope='module')
def daily(rstring, kdbpar):
    return Data(rstring + '/start/db::daily')


def test_resource_doesnt_bork(daily):
    assert repr(daily)


def test_field(daily, kdbpar):
    expr = daily.price
    expected = kdbpar.eval('select price from daily').squeeze()
    result = compute(expr)
    assert result.name == expected.name
    tm.assert_series_equal(result, expected)


def test_field_name(daily):
    qresult = daily.price
    names = repr(qresult).split('\n')[0].strip().split()
    assert len(names) == 1
    assert names[0] == 'price'


def test_simple_op(daily, kdbpar):
    expr = daily.price + 1
    result = compute(expr)
    expected = kdbpar.eval('select price + 1 from daily').squeeze()
    tm.assert_series_equal(result, expected)


def test_complex_date_op_repr(daily, kdbpar):
    sym = bz.symbol('daily', daily.dshape)
    result = by(sym.date.month,
                cnt=sym.nrows,
                size=sym.size.sum(),
                wprice=wavg(sym.size, sym.price))
    assert repr(result)


def test_complex_date_op(daily, kdbpar):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by date from daily
    expr = by(daily.date,
              cnt=daily.price.count(),
              size=daily.size.sum(),
              wprice=wavg(daily.size, daily.price))
    assert repr(expr)
    result = compute(expr)
    query = ('select cnt: count price, size: sum size, wprice: size wavg price '
             '  by date from daily')
    expected = kdbpar.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_complex_nondate_op(daily, kdbpar):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by sym from daily
    expr = by(daily.sym,
              cnt=daily.price.count(),
              size=daily.size.sum(),
              wprice=wavg(daily.size, daily.price))
    assert repr(expr)
    result = compute(expr)
    query = ('select cnt: count price, size: sum size, wprice: size wavg price'
             ' by sym from daily')
    expected = kdbpar.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_is_standard(daily):
    assert is_standard(daily)


def test_by_mean(daily, kdbpar):
    expr = by(daily.sym, price=daily.price.mean())
    expected = kdbpar.eval('select avg price by sym from daily').reset_index()
    result = compute(expr)
    tm.assert_frame_equal(result, expected)


def test_sum_after_subset(daily):
    r = daily[(daily.date == daily.date[-1]) & (daily.sym == 'IBM')]
    result = compute(r.price.sum())
    expected = into(pd.Series, r.price).sum()
    assert result == expected


def test_nrows(daily):
    assert compute(daily.nrows) == compute(daily.date.nrows)


def test_nunique(daily, kdbpar):
    expr = daily.sym.nunique()
    result = compute(expr)
    expected = kdbpar.eval('count distinct exec sym from daily')
    assert result == expected


def test_dateattr_nrows(daily):
    assert compute(daily.nrows) == compute(daily.date.day.nrows)


def test_kq_as_resource(kdb):
    result = Data(kdb)
    for field in result.fields:
        assert isinstance(getattr(result, field), Field)
