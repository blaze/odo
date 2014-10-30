import pytest
import pandas.util.testing as tm
from qpython.qcollection import QException
from blaze import Data, compute
from blaze.compute.core import swap_resources_into_scope
from kdbpy.compute.qtable import ispartitioned


@pytest.fixture
def trade(rstring, kdbpar):
    return Data(rstring + '/start/db::trade', engine=kdbpar)


def separate(expr):
    return swap_resources_into_scope(expr, {})


def test_ispartitioned(trade):
    assert ispartitioned(trade)


@pytest.mark.xfail(raises=QException,
                   reason="We can't handle partitioned tables yet")
def test_head(trade):
    qexpr = trade.head()
    expr, data = separate(qexpr)
    result = compute(expr, data)
    expected = trade.data.engine.eval('.Q.ind[trade; til 10]').squeeze()
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=QException,
                   reason="We can't handle partitioned tables yet")
def test_repr(trade):
    assert repr(trade)


@pytest.mark.xfail(raises=QException,
                   reason="We can't handle partitioned tables yet")
def test_field(trade):
    qexpr = trade.price
    expr, data = separate(qexpr)
    result = compute(expr, data)
    expected = trade.data.engine.eval('select price from trade').squeeze()
    tm.assert_series_equal(result, expected)


@pytest.mark.xfail(raises=QException,
                   reason="We can't handle partitioned tables yet")
def test_field_repr(trade):
    qexpr = trade.price
    assert repr(qexpr)
