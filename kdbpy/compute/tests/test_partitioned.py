import pytest
pytest.importorskip('blaze')
import pandas as pd
import pandas.util.testing as tm
from blaze import Data, compute, into, by
from blaze.compute.core import swap_resources_into_scope
from kdbpy.compute.qtable import ispartitioned


@pytest.fixture
def trade(rstring, kdbpar):
    return Data(rstring + '/start/db::trade', engine=kdbpar)


def separate(expr):
    return swap_resources_into_scope(expr, {})


def test_ispartitioned(trade):
    assert ispartitioned(trade)


def test_head(trade):
    qexpr = trade.head()
    expr, data = separate(qexpr)
    result = compute(expr, data)
    expected = trade.data.engine.eval('.Q.ind[trade; til 10]').squeeze()
    tm.assert_frame_equal(result, expected)


def test_repr(trade):
    assert repr(trade)


def test_field(trade):
    qexpr = trade.price
    expr, data = separate(qexpr)
    result = compute(expr, data)
    expected = trade.data.engine.eval('select price from trade').squeeze()
    tm.assert_series_equal(result, expected)


def test_field_repr(trade):
    qexpr = trade.price
    assert repr(qexpr)


def test_simple_arithmetic(trade):
    qexpr = trade.price + 1 * 2
    result = into(pd.Series, qexpr)
    expected = into(pd.Series, trade.price) + 1 * 2
    tm.assert_series_equal(result, expected)


def test_simple_by(trade):
    qexpr = by(trade.sym, w=(trade.price * trade.size).sum())
    assert repr(qexpr)
