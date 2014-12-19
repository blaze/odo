import pytest

import pandas.util.testing as tm

from blaze import Data, compute, by

from qpython.qcollection import QException

from kdbpy.compute.qtable import is_partitioned
from kdbpy.tests import assert_series_equal


@pytest.fixture(scope='module')
def db(kdbpar):
    return Data(kdbpar)


def test_is_partitioned(db):
    assert is_partitioned(db.trade)
    assert is_partitioned(db.quote)
    assert not is_partitioned(db.nbbo_t)


def test_projection(db):
    expr = db.trade[['price', 'sym']]
    result = compute(expr)
    expected = db.data.eval('select price, sym from trade')
    tm.assert_frame_equal(result, expected)


def test_head(db):
    expr = db.trade.head()
    result = compute(expr)
    expected = db.data.eval('.Q.ind[trade; til 10]')
    tm.assert_frame_equal(result, expected)


def test_repr(db):
    assert repr(db.trade)


def test_field(db):
    expr = db.trade.price
    result = compute(expr)
    expected = db.data.eval('select price from trade').squeeze()
    assert_series_equal(result, expected)


def test_simple_by(db):
    expr = by(db.trade.sym, w=db.trade.price.mean())
    result = compute(expr)
    query = 'select w: avg price by sym from trade'
    expected = db.data.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_selection(db):
    expr = db.trade[db.trade.sym == 'AAPL']
    result = compute(expr)
    expected = db.data.eval('select from trade where sym = `AAPL')
    tm.assert_frame_equal(result, expected)


def test_partitioned_nrows_on_virtual_column(db):
    assert compute(db.quote.nrows) == compute(db.quote.date.nrows)


@pytest.mark.xfail(raises=QException)
def test_field_head(db):
    result = compute(db.trade.price.head(5))
    query = 'exec price from select price from .Q.ind[trade; til 5]'
    expected = db.data.eval(query)
    assert_series_equal(result, expected, check_exact=True)


@pytest.mark.xfail(raises=QException,
                   reason='field expressions not working')
def test_simple_arithmetic(db):
    expr = db.trade.price + 1 * 2
    result = compute(expr)
    expected = db.data.eval('select ((price + 1) * 2) from trade').squeeze()
    assert_series_equal(result, expected)


@pytest.mark.xfail(raises=QException, reason='not yet implemented')
def test_nunique(db):
    expr = db.trade.sym.nunique()
    assert compute(expr) == db.data.eval('count distinct exec sym from '
                                         'select sym from trade')
