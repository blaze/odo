from __future__ import print_function, division, absolute_import

import pytest

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import blaze as bz
from blaze import compute, into, by, discover, dshape, summary
from kdbpy.compute.q import List, Symbol, Dict, String


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


def test_complex_broadcast(t, q, df):
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
    # q) select name, amount_sum: sum amount from t
    expr = by(t.name, t.amount.sum())
    qresult = compute(expr, q)
    result = into(pd.DataFrame, qresult).reset_index()

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


def test_mean(t, q, df):
    expr = t.amount.mean()
    result = compute(expr, q)
    expected = compute(expr, df)
    assert result == expected


def test_std(t, q, df):
    expr = t.amount.std(unbiased=True)
    result = compute(expr, q)
    expected = compute(expr, df)
    np.testing.assert_allclose(result, expected)


def test_var(t, q, df):
    expr = t.amount.var(unbiased=True)
    result = compute(expr, q)
    expected = compute(expr, df)
    np.testing.assert_allclose(result, expected)


def test_max(t, q, df):
    expr = t.id.max()
    result = compute(expr, q)
    expected = compute(expr, df)
    assert result == expected


def test_min(t, q, df):
    expr = t.id.min()
    result = compute(expr, q)
    expected = compute(expr, df)
    assert result == expected


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='Join not implemented for QTables')
def test_simple_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st)
    result = into(pd.DataFrame, compute(expr, {st: sq, rt: rq}))
    expected = compute(expr, {st: sdf, rt: rdf})
    tm.assert_frame_equal(result, expected)


def test_sort(t, q, df):
    expr = t.sort('name')
    result = compute(expr, q)

    # q doesn't keep index order
    expected = df.sort('name', kind='mergesort').reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_nrows(t, q, df):
    qresult = compute(t.nrows(), q)
    expected = len(df)
    assert qresult == expected


@pytest.mark.xfail(raises=AttributeError,
                   reason='Issue with Data resource on our side')
def test_resource(q):
    data = bz.Data(q, name=q.tablename)
    result = data.amount + 1
    assert repr(result)


def test_discover(q):
    assert (str(discover(q)) ==
            str(dshape('var * {name: string, id: int64, amount: float64}')))


def test_into_from_keyed(rq, rdf):
    result = into(pd.DataFrame, rdf)
    tm.assert_frame_equal(result, rdf)


def test_into_from_qtable(q, df):
    result = into(pd.DataFrame, df)
    tm.assert_frame_equal(result, df)


def test_slice(t, q, df):
    expr = t[2:5]
    qresult = compute(expr, q)
    tm.assert_frame_equal(qresult, compute(expr, df).reset_index(drop=True))


@pytest.mark.xfail(raises=AssertionError,
                   reason='Logic for negative slices not worked out yet')
def test_neg_slice(t, q, df):
    expr = t[-2:]
    qresult = compute(expr, q)
    tm.assert_frame_equal(qresult, compute(expr, df).reset_index(drop=True))


@pytest.mark.xfail(raises=AssertionError,
                   reason='Logic for negative slices not worked out yet')
def test_neg_bounded_slice(t, q, df):
    # this should be empty in Q, though it's possible to do this
    expr = t[-2:5]
    qresult = compute(expr, q)
    tm.assert_frame_equal(qresult, compute(expr, df).reset_index(drop=True))




def test_distinct(t, q, df):
    expr = t.name.distinct()
    result = compute(expr, q)
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)
