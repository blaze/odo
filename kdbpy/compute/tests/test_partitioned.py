import pytest

import pandas.util.testing as tm

from blaze import Data, compute, by

from qpython.qcollection import QException

from kdbpy.compute.qtable import is_partitioned
from kdbpy.tests import assert_series_equal


@pytest.fixture(scope='module')
def quote(rstring, kdbpar):
    return Data(rstring + '/start/db::quote')


@pytest.fixture(scope='module')
def trade(rstring, kdbpar):
    return Data(rstring + '/start/db::trade')


def test_is_partitioned(trade):
    assert is_partitioned(trade)


def test_projection(trade, kdbpar):
    expr = trade[['price', 'sym']]
    result = compute(expr)
    expected = kdbpar.eval('select price, sym from trade')
    tm.assert_frame_equal(result, expected)


def test_head(trade, kdbpar):
    expr = trade.head()
    result = compute(expr)
    expected = kdbpar.eval('.Q.ind[trade; til 10]')
    tm.assert_frame_equal(result, expected)


def test_repr(trade):
    assert repr(trade)


def test_field(trade, kdbpar):
    expr = trade.price
    result = compute(expr)
    expected = kdbpar.eval('select price from trade').squeeze()
    assert_series_equal(result, expected)


def test_simple_by(trade, kdbpar):
    expr = by(trade.sym, w=trade.price.mean())
    result = compute(expr)
    query = 'select w: avg price by sym from trade'
    expected = kdbpar.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_selection(trade, kdbpar):
    expr = trade[trade.sym == 'AAPL']
    result = compute(expr)
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
    expr = trade.price + 1 * 2
    result = compute(expr)
    expected = trade.eval('exec price from select ((price + 1) * 2) from trade')
    assert_series_equal(result, expected)


@pytest.mark.xfail(raises=QException, reason='not yet implemented')
def test_nunique(trade, kdbpar):
    expr = trade.sym.nunique()
    assert compute(expr) == kdbpar.eval('count distinct exec sym from '
                                        'select sym from trade')
