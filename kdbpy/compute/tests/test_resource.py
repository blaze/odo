import pytest

import pandas as pd
import pandas.util.testing as tm
bz = pytest.importorskip('blaze')
from toolz import first
from blaze import Data, by, into, compute
from blaze.compute.core import swap_resources_into_scope
from kdbpy.compute.qtable import issplayed, isstandard


@pytest.fixture
def daily(rstring, kdbpar):
    return Data(rstring + '/start/db::daily', engine=kdbpar)


@pytest.fixture
def nbbo(rstring, kdbpar):
    return Data(rstring + '/start/db::nbbo_t', engine=kdbpar)


def test_resource_doesnt_bork(daily):
    assert repr(daily)


def test_field(daily):
    qresult = daily.price
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = compute(expr, first(daily.values()))
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
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = into(pd.DataFrame(columns=expr.fields), compute(expr, daily))
    tm.assert_frame_equal(result, expected)


def test_complex_date_op_repr(daily, kdb):
    sym = bz.Symbol('daily', daily.dshape)
    result = by(sym.date.month,
                cnt=sym.nrows,
                size=sym.size.sum(),
                wprice=(sym.price * sym.size).sum() / sym.price.count())
    assert repr(result)


@pytest.mark.xfail(raises=AssertionError, reason='Blaze does not support '
                   'non child reductions yet')
def test_complex_date_op(daily):
    result = by(daily.date,
                cnt=daily.price.count(),
                size=daily.size.sum(),
                wprice=(daily.price * daily.size).sum() / daily.price.sum()
                )
    expr, data = swap_resources_into_scope(result, {})
    df = into(pd.DataFrame, data[expr._leaves()[0]]).set_index('date')
    size = df.size.resample('D', how='sum')
    wprice = df[['price',
                 'size']].resample('D',
                                   how=lambda x: (x.price * x.size).sum()).price
    weights = df['price'].resample('D', how='sum')
    wprice /= weights
    cnt = df.price.resample('D', how=lambda x: len(x))
    expected = pd.DataFrame({'cnt': cnt, 'size': size, 'wprice': wprice})
    expected = expected[['cnt', 'size', 'wprice']].dropna()

    assert repr(result)
    tm.assert_frame_equal(into(pd.DataFrame, result), expected)


def test_complex_nondate_op(daily):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by sym from daily
    qresult = by(daily.sym,
                 cnt=daily.nrows,
                 size=daily.size.sum(),
                 wprice=(daily.price * daily.size).sum() / daily.price.sum())
    assert repr(qresult)
    result = into(pd.DataFrame, qresult)
    expr, daily = swap_resources_into_scope(qresult, {})
    expected = compute(expr, first(daily.values()))
    tm.assert_frame_equal(result, expected)


def test_issplayed(nbbo):
    assert issplayed(nbbo)


def test_isstandard(daily):
    assert isstandard(daily)
