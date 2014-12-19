from __future__ import print_function

import pytest

import pandas as pd
import pandas.util.testing as tm

from into import into

import blaze as bz
from blaze import Data, by, compute
from blaze.expr import Field

from kdbpy.compute.qtable import is_standard


def wsum(x, y):
    return (x * y).sum()


def wavg(x, y):
    return wsum(x, y) / x.sum()


@pytest.fixture(scope='module')
def std(kdbpar):
    return Data(kdbpar)


@pytest.fixture(scope='module')
def db(kdb):
    return Data(kdb)


def test_resource_doesnt_bork(std):
    assert repr(std.daily)


def test_field(std):
    expr = std.daily.price
    expected = std.data.eval('select price from daily').squeeze()
    result = compute(expr)
    assert result.name == expected.name
    tm.assert_series_equal(result, expected)


def test_field_name(std):
    result = std.daily.price
    names = repr(result).split('\n')[0].strip().split()
    assert len(names) == 1
    assert names[0] == 'price'


def test_simple_op(std):
    expr = std.daily.price + 1
    result = compute(expr)
    expected = std.data.eval('select price + 1 from daily').squeeze()
    tm.assert_series_equal(result, expected)


def test_complex_date_op_repr(std):
    sym = bz.symbol('daily', std.daily.dshape)
    result = by(sym.date.month,
                cnt=sym.nrows,
                size=sym.size.sum(),
                wprice=wavg(sym.size, sym.price))
    assert repr(result)


def test_complex_date_op(std):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by date from daily
    expr = by(std.daily.date,
              cnt=std.daily.price.count(),
              size=std.daily.size.sum(),
              wprice=wavg(std.daily.size, std.daily.price))
    assert repr(expr)
    result = compute(expr)
    query = ('select cnt: count price, size: sum size, wprice: size wavg price '
             '  by date from daily')
    expected = std.data.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_complex_nondate_op(std):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by sym from daily
    expr = by(std.daily.sym,
              cnt=std.daily.price.count(),
              size=std.daily.size.sum(),
              wprice=wavg(std.daily.size, std.daily.price))
    assert repr(expr)
    result = compute(expr)
    query = ('select cnt: count price, size: sum size, wprice: size wavg price'
             ' by sym from daily')
    expected = std.data.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_is_standard(std):
    assert is_standard(std.daily)


def test_by_mean(std):
    expr = by(std.daily.sym, price=std.daily.price.mean())
    expected = std.data.eval('select avg price by sym from daily').reset_index()
    result = compute(expr)
    tm.assert_frame_equal(result, expected)


def test_sum_after_subset(std):
    r = std.daily[(std.daily.date == std.daily.date[-1]) &
                  (std.daily.sym == 'IBM')]
    result = compute(r.price.sum())
    expected = into(pd.Series, r.price).sum()
    assert result == expected


def test_nrows(std):
    assert compute(std.daily.nrows) == compute(std.daily.date.nrows)


def test_nunique(std):
    expr = std.daily.sym.nunique()
    result = compute(expr)
    expected = std.data.eval('count distinct exec sym from daily')
    assert result == expected


def test_dateattr_nrows(std):
    assert compute(std.daily.nrows) == compute(std.daily.date.day.nrows)


@pytest.mark.xfail(raises=AttributeError,
                   reason="'Field' object has no attribute 'name'")
def test_foreign_key(db):
    expr = db.market.ex_id.name
    result = compute(expr)
    expected = std.data.eval('select ex_id.name from market').squeeze()
    assert_series_equal(result, expected)
