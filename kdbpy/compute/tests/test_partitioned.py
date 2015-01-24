import pytest

import pandas as pd
import pandas.util.testing as tm

from blaze import compute, by, into

from qpython.qcollection import QException

from kdbpy.compute.qtable import is_partitioned
from kdbpy.tests import assert_series_equal


def test_is_partitioned(par):
    assert is_partitioned(par.trade)
    assert is_partitioned(par.quote)
    assert not is_partitioned(par.nbbo_t)


def test_projection(par):
    expr = par.trade[['price', 'sym']]
    result = compute(expr)
    expected = par.data.eval('select price, sym from trade')
    tm.assert_frame_equal(result, expected)


def test_head(par):
    expr = par.trade.head()
    result = compute(expr)
    expected = par.data.eval('.Q.ind[trade; til 10]')
    tm.assert_frame_equal(result, expected)


def test_repr(par):
    assert repr(par.trade)


def test_field(par):
    expr = par.trade.price
    result = compute(expr)
    expected = par.data.eval('select price from trade').squeeze()
    assert_series_equal(result, expected)


def test_simple_by(par):
    expr = by(par.trade.sym, price=par.trade.price.mean())
    result = compute(expr)
    query = 'select avg price by sym from trade'
    expected = par.data.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_selection(par):
    expr = par.trade[par.trade.sym == 'AAPL']
    result = compute(expr)
    expected = par.data.eval('select from trade where sym = `AAPL')
    tm.assert_frame_equal(result, expected)


def test_partitioned_nrows_on_virtual_column(par):
def test_nunique(par):
    expr = par.trade.sym.nunique()
    qs = 'count distinct exec sym from select sym from trade'
    assert compute(expr) == par.data.eval(qs)


@pytest.mark.xfail(raises=QException,
                   reason="partitioned tables don't yet work with comparisons")
def test_any(par):
    price = par.trade.price
    expr = (price > 50) & (price < 100)
    qs = ('first exec price from select price: any[price within 50 100]'
          '  from trade')
    expected = par.data.eval(qs)
    result = compute(expr.any())
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=QException,
                   reason="partitioned tables don't yet work with comparisons")
def test_all(par):
    price = par.trade.price
    expr = (price > 0) & (price < 100000)
    qs = ('first exec price from select price: all[price within 0 100000]'
          '  from trade')
    expected = par.data.eval(qs)
    result = compute(expr.all())
    tm.assert_frame_equal(result, expected)


agg_funcs = {'mean': 'avg'}


@pytest.mark.parametrize('agg', ['mean', 'sum', 'count', 'min', 'max'])
def test_agg(par, agg):
    expr = getattr(par.trade.price, agg)()
    qs = ('first exec price from select %s price from trade' %
          agg_funcs.get(agg, agg))
    assert compute(expr) == par.data.eval(qs)


def test_nrows_on_virtual_column(par):
    assert compute(par.quote.nrows) == compute(par.quote.date.nrows)


@pytest.mark.xfail(raises=QException)
def test_field_head(par):
    result = compute(par.trade.price.head(5))
    query = 'exec price from .Q.ind[trade; til 5]'
    expected = par.data.eval(query)
    assert_series_equal(result, expected, check_exact=True)


@pytest.mark.xfail(raises=QException,
                   reason='field expressions not working')
def test_simple_arithmetic(par):
    expr = par.trade.price + 1 * 2
    result = compute(expr)
    expected = par.data.eval('select (price + 1) * 2 from trade').squeeze()
    assert_series_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='not implemented for partitioned tables')
def test_append_frame_to_partitioned(par):
    tablename = par.trade._name
    df = par.data.eval('select from %s' % tablename)
    expected = pd.concat([df, df], ignore_index=True)
    result = into(pd.DataFrame, into(par.trade, df))
    tm.assert_frame_equal(result, expected)
