from __future__ import print_function
import pytest

import pandas as pd
import pandas.util.testing as tm

from toolz import first

import blaze as bz
from blaze import Data, by, into, compute
from blaze.expr import Field
from blaze.compute.core import swap_resources_into_scope

from kdbpy.compute.qtable import is_splayed, is_standard


@pytest.fixture(scope='module')
def daily(rstring, kdbpar):
    return Data(rstring + '/start/db::daily')


@pytest.fixture(scope='module')
def quote(rstring, kdbpar):
    return Data(rstring + '/start/db::quote')


@pytest.fixture(scope='module')
def nbbo(rstring, kdbpar):
    return Data(rstring + '/start/db::nbbo_t')


def test_resource_doesnt_bork(daily):
    assert repr(daily)


def test_field(daily):
    qresult = daily.price
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = compute(expr, into(pd.DataFrame, first(daily.values())))
    result = into(pd.Series, qresult)
    assert result.name == expected.name
    tm.assert_series_equal(result, expected)


def test_field_name(daily):
    qresult = daily.price
    names = repr(qresult).split('\n')[0].strip().split()
    assert len(names) == 1
    assert names[0] == 'price'


def test_simple_op(daily):
    qresult = daily.price + 1
    result = into(pd.DataFrame, qresult)
    df = into(pd.DataFrame, daily)
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = into(pd.DataFrame(columns=expr.fields), compute(expr, df))
    tm.assert_frame_equal(result, expected)


def test_complex_date_op_repr(daily, kdb):
    sym = bz.Symbol('daily', daily.dshape)
    result = by(sym.date.month,
                cnt=sym.nrows,
                size=sym.size.sum(),
                wprice=(sym.price * sym.size).sum() / sym.price.count())
    assert repr(result)


def test_complex_date_op(daily):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by date from daily
    qresult = by(daily.date,
                 cnt=daily.price.count(),
                 size=daily.size.sum(),
                 wprice=(daily.size * daily.price).sum() / daily.price.sum())
    result = sorted(into(list, into(pd.DataFrame, qresult).reset_index()))
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = sorted(compute(expr, into(list, into(pd.DataFrame,
                                                    first(daily.values())))))
    assert result == expected


def test_complex_nondate_op(daily):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by sym from daily
    qresult = by(daily.sym,
                 cnt=daily.price.count(),
                 size=daily.size.sum(),
                 wprice=(daily.size * daily.price).sum() / daily.price.sum())
    assert repr(qresult)
    result = sorted(into(list, into(pd.DataFrame, qresult).reset_index()))
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = sorted(compute(expr, into(list, into(pd.DataFrame,
                                                    first(daily.values())))))
    assert result == expected


def test_is_splayed(nbbo):
    assert is_splayed(nbbo)


def test_is_standard(daily):
    assert is_standard(daily)


def test_by_mean(daily):
    qresult = by(daily.sym, price=daily.price.mean())
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = compute(expr, into(pd.DataFrame, first(daily.values())))
    expected = expected.set_index('sym')
    result = into(pd.DataFrame, qresult)
    tm.assert_frame_equal(result, expected)


def test_sum_after_subset(daily):
    r = daily[(daily.date == daily.date[-1]) & (daily.sym == 'IBM')]
    result = compute(r.price.sum())
    expected = into(pd.Series, r.price).sum()
    assert result == expected


def nrows(x):
    return into(int, x.nrows)


def test_nrows(daily):
    assert nrows(daily) == nrows(daily.date)


def test_nunique(daily):
    expr, data = swap_resources_into_scope(daily.sym.nunique(), {})
    assert compute(expr, data) == compute(expr, into(pd.Series, daily.sym))


def test_splayed_nrows(nbbo):
    assert nrows(nbbo) == nrows(nbbo.sym)


def test_dateattr_nrows(daily):
    assert nrows(daily) == nrows(daily.date.day)


def test_splayed_time_type(nbbo):
    assert nrows(nbbo) == nrows(nbbo.time)


def test_partitioned_nrows_on_virtual_column(quote):
    assert nrows(quote) == nrows(quote.date)


def test_kq_as_resource(kdb):
    result = Data(kdb)
    for field in result.fields:
        assert isinstance(getattr(result, field), Field)
