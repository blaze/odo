import pytest

import pandas.util.testing as tm

from blaze import Data, compute, by
from blaze.compute.core import swap_resources_into_scope

from qpython.qcollection import QException

from kdbpy.compute.qtable import is_partitioned
from kdbpy.tests import assert_series_equal


@pytest.fixture(scope='module')
def quote(rstring, kdbpar):
    return Data(rstring + '/start/db::quote')


@pytest.fixture(scope='module')
def trade(rstring, kdbpar):
    return Data(rstring + '/start/db::trade')


def separate(expr):
    return swap_resources_into_scope(expr, {})


def test_is_partitioned(trade):
    assert is_partitioned(trade)


def test_projection(trade, kdbpar):
    qexpr = trade[['price', 'sym']]
    result = compute(qexpr)
    expected = kdbpar.eval('select price, sym from trade')
    tm.assert_frame_equal(result, expected)


def test_head(trade, kdbpar):
    qexpr = trade.head()
    expr, data = separate(qexpr)
    result = compute(expr, data)
    expected = kdbpar.eval('.Q.ind[trade; til 10]')
    tm.assert_frame_equal(result, expected)


def test_repr(trade):
    assert repr(trade)


def test_field(trade, kdbpar):
    qexpr = trade.price
    expr, data = separate(qexpr)
    result = compute(qexpr)
    expected = kdbpar.eval('select price from trade').squeeze()
    assert_series_equal(result, expected)


def test_simple_by(trade, kdbpar):
    qexpr = by(trade.sym, w=trade.price.mean())
    expr, data = separate(qexpr)
    result = compute(qexpr)
    query = 'select w: avg price by sym from trade'
    expected = kdbpar.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_selection(trade, kdbpar):
    qexpr = trade[trade.sym == 'AAPL']
    expr, data = separate(qexpr)
    result = compute(qexpr)
    expected = kdbpar.eval('select from trade where sym = `AAPL')
    tm.assert_frame_equal(result, expected)


def test_partitioned_nrows_on_virtual_column(quote):
    assert compute(quote.nrows) == compute(quote.date.nrows)


@pytest.mark.xfail(raises=QException)
def test_field_head(trade, kdbpar):
    result = compute(trade.price.head(5))
    query = 'exec price from select price from .Q.ind[trade; til 5]'
    expected = kdbpar.eval(query)
    assert_series_equal(result, expected, check_exact=True)


@pytest.mark.xfail(raises=QException,
                   reason='field expressions not working')
def test_simple_arithmetic(trade):
    qexpr = trade.price + 1 * 2
    result = compute(qexpr)
    expected = trade.eval('exec price from select ((price + 1) * 2) from trade')
    assert_series_equal(result, expected)


@pytest.mark.xfail(raises=QException, reason='not yet implemented')
def test_nunique(trade, kdbpar):
    expr = trade.sym.nunique()
    assert compute(expr) == kdbpar.eval('count distinct exec sym from '
                                        'select sym from trade')
