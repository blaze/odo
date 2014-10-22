from __future__ import print_function, division, absolute_import
import pytest

from numpy import nan
import pandas as pd
import pandas.util.testing as tm
import blaze as bz
from blaze import compute, into, by
from kdbpy.compute import Q
from kdbpy.compute.q import List, Symbol, Dict, String
from kdbpy.pcl import PCL


@pytest.fixture
def t():
    return bz.Symbol('t', 'var * {id: int, amount: float64, name: string}')


@pytest.fixture(scope='module')
def kdb():
    r = PCL()
    r.kdb.eval('t: ([] id: 1 2 3; amount: -100.90 0n 432.2; '
               'name: `Bob`Alice`Joe; street: ("maple"; "apple"; "pine"))')
    return r


@pytest.fixture
def q(t, kdb):
    return Q(t, q=[], kdb=kdb)


@pytest.fixture
def df():
    return pd.DataFrame([(1, -100.90, 'Bob', 'maple'),
                         (2, nan, 'Alice', 'apple'),
                         (3, 432.2, 'Joe', 'pine')],
                        columns=['id', 'amount', 'name', 'street'])


def test_qlist():
    x = List(Symbol('a'), 1, 2)
    assert repr(x) == '(`a; 1; 2)'

    x = List(Symbol('a sym'), List(Symbol('a')), 3)
    assert repr(x) == '(`$"a sym"; (enlist[`a]); 3)'


def test_qdict():
    x = Dict([(Symbol('a'), 1), (Symbol('b'), 2)])
    assert repr(x) == '(`a; `b)!(1; 2)'


def test_qsymbol():
    s = Symbol('a')
    assert repr(s) == '`a'

    s = Symbol('a symbol')
    assert repr(s) == '`$"a symbol"'


def test_qstring():
    s = String('s')
    assert repr(s) == '"s"'

    s = String('"s"')
    assert repr(s) == '"\"s\""'


def test_projection(t, q, df):
    expr = t[['id', 'amount']]
    expected = compute(expr, df)
    result = into(pd.DataFrame, compute(expr, q))
    tm.assert_frame_equal(result, expected)


def test_single_projection(t, q, df):
    expr = t[['id']]
    result = into(pd.DataFrame, compute(expr, q))
    tm.assert_frame_equal(result, compute(expr, df))


def test_selection(t, q, df):
    expr = t[t.id == 1]
    result = into(pd.DataFrame, compute(expr, q))
    expected = compute(expr, df)
    tm.assert_frame_equal(result, expected)


def test_broadcast(t, q, df):
    expr = t.id + 1
    result = into(pd.Series, compute(expr, q))
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)


def test_complex_arith(t, q, df):
    expr = t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3
    result = into(pd.Series, compute(expr, q))
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)


def test_complex_selection(t, q, df):
    expr = t[t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3]

    # q doesn't know anything pandas indexes
    result = into(pd.DataFrame, compute(expr, q))

    # but blaze preserves them
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_complex_selection_projection(t, q, df):
    expr = t[t.id ** 2 + t.amount > t.id - 3][['id', 'amount']]

    # q doesn't know anything pandas indexes
    result = into(pd.DataFrame, compute(expr, q))

    # but blaze preserves them
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_unary_op(t, q, df):
    expr = -t.amount
    result = into(pd.Series, compute(expr, q))
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)


def test_string_compare(t, q, df):
    expr = t.name == 'Alice'
    result = into(pd.Series, compute(expr, q))
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)


def test_simple_by(t, q, df):
    expr = by(t.name, t.amount.sum())
    result = into(pd.DataFrame, compute(expr, q)).reset_index()

    # q fills NaN reducers with 0
    expected = compute(expr, df).fillna(0)
    tm.assert_frame_equal(result, expected)

    result = into(pd.DataFrame, compute(expr, q))
    expected = compute(expr, df).set_index('name', drop=True).fillna(0)
    tm.assert_frame_equal(result, expected)


def test_field(t, q, df):
    expr = t.name
    result = compute(expr, q)
    tm.assert_series_equal(pd.Series(result), compute(expr, df))


def test_sum(t, q, df):
    expr = t.amount.sum()
    result = compute(expr, q)
    assert result == compute(expr, df)


def test_count(t, q, df):
    expr = t.amount.count()
    result = compute(expr, q)
    assert result == compute(expr, df)
