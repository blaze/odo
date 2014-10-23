from __future__ import print_function, division, absolute_import

import pytest

import getpass

from numpy import nan
import pandas as pd
import pandas.util.testing as tm
import blaze as bz
from blaze import compute, into, by
from kdbpy.compute import tables, QTable
from kdbpy.compute.q import List, Symbol, Dict, String
from kdbpy.kdb import Credentials, KQ


@pytest.fixture(scope='module')
def t():
    return bz.Symbol('t', 'var * {name: string, id: int64, amount: float64}')


@pytest.fixture(scope='module')
def rt():
    return bz.Symbol('rt', 'var * {name: string, tax: float64, street: string}')


@pytest.fixture(scope='module')
def st():
    return bz.Symbol('st', 'var * {name: string, jobcode: int64, tree: string}')


ports = [5001]


@pytest.yield_fixture
def port():
    yield ports[-1]
    ports.append(ports[-1] + 1)


@pytest.fixture(scope='module')
def rstring(port):
    return 'kdb://pcloud@localhost:%d::t' % port


@pytest.yield_fixture(scope='module')
def kdb(port):
    r = KQ(Credentials(username=getpass.getuser(), password=None,
                       host='localhost', port=port))
    r.start()
    r.eval('t: ([] name: `Bob`Alice`Joe; id: 1 2 3; amount: -100.90 0n 432.2)')
    r.eval('rt: ([name: `Bob`Alice`Joe`John] tax: -3.1 2.0 0n 4.2; '
           'street: `maple`apple`pine`grove)')
    r.eval('st: ([name: `Bob`Alice`Joe] jobcode: 9 10 11; '
           'tree: `maple`apple`pine)')
    yield r
    r.stop()


@pytest.fixture(scope='module')
def q():
    return QTable('kdb://pcloud@localhost:5001::t')


@pytest.fixture
def df():
    return pd.DataFrame([('Bob', 1, -100.90),
                         ('Alice', 2, nan),
                         ('Joe', 3, 432.2)],
                        columns=['name', 'id', 'amount'])


@pytest.fixture
def rdf():
    return pd.DataFrame([('Bob', -3.1, 'maple'),
                         ('Alice', 2.0, 'apple'),
                         ('Joe', nan, 'pine'),
                         ('John', 4.2, 'grove')],
                        columns=['name', 'tax', 'street']).set_index('name',
                                                                     drop=True)


@pytest.fixture
def sdf():
    return pd.DataFrame([('Bob', 9, 'maple'),
                         ('Alice', 10, 'apple'),
                         ('Joe', 11, 'pine')]).set_index('name', drop=True)


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
    assert result == len(df)


@pytest.mark.xfail
def test_simple_join(rt, st, q, rdf, sdf):
    expr = bz.join(t, rt)
    result = into(pd.DataFrame, compute(expr, q))
    expected = compute(expr, df, rdf)
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=ValueError, reason='single q process')
def test_tables(kdb, rt, st, t):
    assert tables(kdb) == dict(zip(['rt', 'st', 't'], [rt, st, t]))
