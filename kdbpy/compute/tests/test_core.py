from __future__ import print_function, division, absolute_import

import string
import pytest

import numpy as np

import pandas as pd
import pandas.util.testing as tm
from datashape import null, dshape, Record, var

import blaze as bz
from blaze import compute, into, by, discover, dshape, summary, Data, symbol
from kdbpy import QTable
from kdbpy.compute.qtable import qtypes


def test_projection(t, q, df):
    expr = t[['id', 'amount']]
    expected = compute(expr, df)
    qresult = compute(expr, q)
    result = into(pd.DataFrame, qresult)
    tm.assert_frame_equal(result, expected)


def test_single_projection(t, q, df):
    expr = t[['id']]
    result = into(pd.DataFrame, compute(expr, q))
    tm.assert_frame_equal(result, compute(expr, df))


def test_selection(t, q, df):
    expr = t[t.id == 1]
    result = into(pd.DataFrame, compute(expr, q))
    expected = compute(expr, df).reset_index(drop=True)
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
    qresult = compute(expr, q)
    result = into(pd.Series, qresult)
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)


def test_simple_by(t, q, df):
    # q) select name, amount_sum: sum amount from t
    expr = by(t.name, amount=t.amount.sum())
    qresult = compute(expr, q)
    result = into(pd.DataFrame, qresult).reset_index()

    # q fills NaN reducers with 0
    expected = compute(expr, df).fillna(0)
    tm.assert_frame_equal(result, expected)

    result = into(pd.DataFrame, compute(expr, q))
    expected = compute(expr, df).set_index('name', drop=True).fillna(0)
    tm.assert_frame_equal(result, expected)


def test_multikey_by(t, q, df):
    expr = by(t[['name', 'on']], amount=t.amount.mean())
    qresult = compute(expr, q)
    result = qresult.reset_index()
    expected = compute(expr, df)
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=AttributeError, reason='Cannot nest joins in '
                   'groupbys yet')
def test_join_then_by(kdb):
    dates_data = kdb['dates'].data
    dates = symbol('dates', discover(dates_data))
    prices_data = kdb['prices'].data
    prices = symbol('prices', discover(prices_data))
    joined = bz.join(dates, prices, on_left='account', on_right='account')
    expr = by(joined.date, amt_mean=joined.amount.mean())
    result = compute(expr, {dates: dates_data, prices: prices_data})
    expected = compute(expr, {dates: kdb.eval('dates'),
                              prices: kdb.eval('prices')})
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


def test_simple_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st)
    result = into(pd.DataFrame, compute(expr, {st: sq, rt: rq}))
    expected = compute(expr, {st: sdf.reset_index(), rt: rdf.reset_index()})
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='Only inner join implemented for QTable')
def test_outer_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st, how='outer')
    result = into(pd.DataFrame, compute(expr, {st: sq, rt: rq}))
    expected = compute(expr, {st: sdf.reset_index(), rt: rdf.reset_index()})
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='Cannot specify different columns')
def test_different_column_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st, on_left='name', on_right='alias')
    result = into(pd.DataFrame, compute(expr, {st: sq, rt: rq}))
    expected = compute(expr, {st: sdf.reset_index(), rt: rdf.reset_index()})
    tm.assert_frame_equal(result, expected)


def test_sort(t, q, df):
    expr = t.sort('name')
    result = compute(expr, q)

    # q doesn't keep index order
    expected = df.sort('name', kind='mergesort').reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_nrows(t, q, df):
    qresult = compute(t.nrows, q)
    expected = len(df)
    assert qresult == expected


@pytest.mark.xfail(raises=ValueError, reason='axis == 1 not supported on record'
                   ' types')
def test_nelements(t, q, df):  # pragma: no cover
    qresult = compute(t.nelements(axis=1), q)
    expected = df.shape[1]
    assert qresult == expected


def test_discover(q):
    assert (str(discover(q)) ==
            str(dshape('var * {name: string, id: int64, amount: float64, '
                       'when: datetime, on: date}')))


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

    expr = t.id[2:5]
    qresult = compute(expr, q)
    tm.assert_series_equal(qresult, compute(expr, df).reset_index(drop=True))


def test_neg_index_series(t, q, df):
    expr = t.amount[-1]
    qresult = compute(expr, q)
    assert qresult == compute(expr, df)


def test_neg_index_frame(t, q, df):
    expr = t[-1]
    qresult = compute(expr, q).squeeze()
    tm.assert_series_equal(qresult, compute(expr, df))


def test_index_row(t, q, df):
    expr = t[1]
    qresult = compute(expr, q).squeeze()
    tm.assert_series_equal(qresult, compute(expr, df))


def test_neg_slice(t, q, df):
    expr = t[-2:]
    qresult = compute(expr, q)
    expected = compute(expr, df)
    expected = expected.reset_index(drop=True)
    tm.assert_frame_equal(qresult, expected)


def test_neg_bounded_slice(t, q, df):
    # this should be empty in Q, though it's possible to do this
    expr = t[-2:5]
    qresult = compute(expr, q)
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(qresult, expected)


def test_neg_bounded_by_negative_slice(t, q, df):
    # this should be empty in Q, though it's possible to do this
    expr = t[-5:-2]
    qresult = compute(expr, q)
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(qresult, expected)


def test_raw_summary(t, q, df):
    expr = summary(s=t.amount.sum(), mn=t.id.mean())
    result = compute(expr, q)
    tm.assert_series_equal(result.squeeze(), compute(expr, df))


def test_simple_summary(t, q):
    expr = by(t.name, s=t.amount.sum())
    result = compute(expr, q)
    qexpected = 'select s: sum amount by name from t'
    expected = q.engine.eval(qexpected)
    tm.assert_frame_equal(result.sort_index(axis=1), expected)


def test_twofunc_summary(t, q):
    expr = by(t.name, s=t.amount.sum(), mn=t.id.mean())
    result = compute(expr, q)
    qexpected = 'select s: sum amount, mn: avg id by name from t'
    expected = q.engine.eval(qexpected)
    tm.assert_frame_equal(result.sort_index(axis=1),
                          expected.sort_index(axis=1))


def test_complex_summary(t, q):
    expr = by(t.name, s=t.amount.sum(), mn=t.id.mean(),
              mx=t.amount.max() + 1)
    result = compute(expr, q)
    qexpected = ('select s: sum amount, mn: avg id, mx: (max amount) + 1 '
                 'by name from t')
    expected = q.engine.eval(qexpected)
    tm.assert_frame_equal(result.sort_index(axis=1),
                          expected.sort_index(axis=1))


def test_distinct(t, q, df):
    expr = t.name.distinct()
    result = compute(expr, q)
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected)


def test_nunique(t, q, df):
    expr = t.name.nunique()
    result = compute(expr, q)
    expected = compute(expr, df)
    assert result == expected


@pytest.mark.parametrize('attr', ['year', 'month', 'day', 'hour', 'minute',
                                  'second', 'millisecond', 'microsecond'])
def test_dates(t, q, df, attr):
    expr = getattr(t.when, attr)
    result = compute(expr, q)
    expected = compute(expr, df)
    tm.assert_series_equal(result, expected, check_dtype=False)


def test_dates_date(t, q, df):
    expr = t.when.date
    result = compute(expr, q)
    expected = compute(expr, df)
    expected = pd.to_datetime(expected)  # pandas returns objects here so coerce
    tm.assert_series_equal(result, expected, check_dtype=False)


def test_by_with_where(t, q, df):
    r = t[t.amount > 1]
    expr = by(r.name, s=r.amount.sum(), m=r.amount.mean())
    result = compute(expr, q)
    expected = compute(expr, df)
    expected = expected.set_index('name')
    tm.assert_frame_equal(result, expected)


def test_by_name(t, q, df):
    expr = by(t.when.day, m=t.amount.mean())
    name = 'when_day'
    result = compute(expr, q)
    expected = compute(expr, df).rename(columns={'index': name})
    tm.assert_frame_equal(result, expected.set_index(name))


def test_by_with_complex_where(t, q, df):
    r = t[((t.amount > 1) & (t.id > 0)) | (t.amount < 4)]
    expr = by(r.name, s=r.amount.sum(), m=r.amount.mean())
    result = compute(expr, q)
    expected = compute(expr, df)
    expected = expected.set_index('name')
    tm.assert_frame_equal(result, expected)


@pytest.mark.parametrize('d, ts',
                         [('2014-01-02', pd.Timestamp('2014-01-02 00:00:00.000000001')),
                          (pd.Timestamp('2014-01-02'), '2014-01-02 00:00:00.000000001')])
def test_datelike_compare(kdb, d, ts):
    name = 'date_t'
    t = bz.Symbol(name, 'var * {d: date, ts: datetime}')
    kdb.eval('%s: ([] d: 2014.01.01 + til 10; '
             'ts: 2014.01.01D00:00:00.000000000 + til 10)' % name)
    df = kdb.eval(name)
    q = QTable(tablename=name, engine=kdb)

    def compare(lhs, rhs):
        expr = t[lhs == rhs]
        result = compute(expr, q)
        expected = compute(expr, df).reset_index(drop=True)
        tm.assert_frame_equal(result, expected)

    compare(t.d, d)
    compare(d, t.d)
    compare(t.ts, ts)
    compare(ts, t.ts)


def test_table_with_timespan(kdb):
    name = 'ts'
    kdb.eval('%s: ([] ts: 00:00:00.000000000 + 1 + til 10; amount: til 10)' %
             name)
    qt = QTable(tablename=name, engine=kdb)
    result = discover(qt)
    expected = dshape('var * {ts: timedelta[unit="ns"], amount: int64}')
    assert expected == result


def test_empty_table(rstring, kdb):
    name = 'no_rows'
    kdb.eval('%s: ([oid: ()] a: (); b: ())' % name)
    d = Data('%s::%s' % (rstring, name))
    expected = dshape(var * Record([('oid', null), ('a', null), ('b', null)]))
    assert discover(d) == expected


def test_empty_all_types(rstring, kdb):
    types = sorted(filter(None, qtypes))
    name = 'all_types'
    names = string.ascii_uppercase[:len(types)]
    single_template = '%s: "%s" $ ()'
    query = '%s: ([] %s)' % (name, '; '.join(single_template % (name, t)
                                             for name, t in zip(names, types)))

    kdb.eval(query)
    d = Data('%s::%s' % (rstring, name))
    s = '{name}: {type}'
    expected = ', '.join(s.format(name=name, type=qtypes[t])
                         for name, t in zip(names, types))
    assert discover(d) == dshape('var * {%s}' % expected)
