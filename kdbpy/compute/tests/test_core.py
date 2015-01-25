from __future__ import print_function, division, absolute_import

import string
import pytest

import numpy as np

import pandas as pd
import pandas.util.testing as tm
from datashape import null, dshape, Record, var

import blaze as bz
from blaze import compute, into, by, discover, dshape, summary, Data
from kdbpy.compute.qtable import qtypes
from kdbpy.tests import assert_series_equal
from into import into


def test_projection(t, q, df):
    expr = t[['id', 'amount']]
    expected = compute(expr, df)
    result = compute(expr, q)
    tm.assert_frame_equal(result, expected)


def test_single_projection(t, q, df):
    expr = t[['id']]
    result = compute(expr, q)
    expected = compute(expr, df)
    tm.assert_frame_equal(result, expected)


def test_selection(t, q, df):
    expr = t[t.id == 1]
    result = compute(expr, q)
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_broadcast(t, q, df):
    expr = t.id + 1
    result = compute(expr, q)
    expected = compute(expr, df)
    assert_series_equal(result, expected)


def test_complex_broadcast(t, q, df):
    expr = t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3
    result = compute(expr, q)
    expected = compute(expr, df)

    # pandas doesn't preserve the name here
    assert_series_equal(result, expected, check_name=False)


def test_complex_selection(t, q, df):
    expr = t[t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3]

    result = compute(expr, q)
    # blaze preserves indexes so we reset them
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_complex_selection_projection(t, q, df):
    expr = t[t.id ** 2 + t.amount > t.id - 3][['id', 'amount']]

    result = compute(expr, q)
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_unary_op(t, q, df):
    expr = -t.amount
    result = compute(expr, q)
    expected = compute(expr, df)
    assert_series_equal(result, expected)


def test_string_compare(t, q, df):
    expr = t.name == 'Alice'
    result = compute(expr, q)
    expected = compute(expr, df)
    assert_series_equal(result, expected)


def test_simple_by(t, q, df):
    # q) select name, amount_sum: sum amount from t
    expr = by(t.name, amount=t.amount.sum())
    result = compute(expr, q)

    # q fills NaN reducers with 0
    expected = compute(expr, df)
    tm.assert_frame_equal(result, expected)

    result = compute(expr, q)
    expected = compute(expr, df)
    tm.assert_frame_equal(result, expected)


def test_multikey_by(t, q, df):
    expr = by(t[['name', 'on']], amount=t.amount.mean())
    result = compute(expr, q)
    expected = compute(expr, df)
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=AttributeError,
                   reason='Cannot nest joins in groupbys yet')
def test_join_then_by(db):
    joined = bz.join(db.dates, db.prices, 'account')
    expr = by(joined.date, amt_mean=joined.amount.mean())

    query = ('select amt_mean: avg amount by date from '
             'ej[`account; dates; prices]')
    expected = db.data.eval(query)
    result = compute(expr)
    tm.assert_frame_equal(result, expected)


def test_field(t, q, df):
    expr = t.name
    result = compute(expr, q)
    assert_series_equal(result, compute(expr, df))


@pytest.mark.parametrize('reduction', ['sum', 'count', 'mean', 'max', 'min',
                                       'nelements'])
def test_reductions(t, q, df, reduction):
    expr = getattr(t.amount, reduction)()
    assert compute(expr, q) == compute(expr, df)


@pytest.mark.parametrize('reduction', ['std', 'var'])
def test_std_var(t, q, df, reduction):
    expr = getattr(t.amount, reduction)()
    np.testing.assert_almost_equal(compute(expr, q), compute(expr, df))


@pytest.mark.xfail(raises=ValueError,
                   reason='axis=1 does not make sense for q right now')
def test_reduction_axis_argument_fails(t, q, df):
    compute(t.amount.mean(axis=1), q)


def test_nrows(t, q, df):
    assert compute(t.nrows, q) == len(df)


def test_column_nrows(db):
    assert compute(db.t.on.nrows) == len(db.t)


def test_date_nrows_in_by_expression(db):
    expr = by(db.t.name, count=db.t.when.nrows)
    result = compute(expr)
    expected = pd.DataFrame([('Alice', 2),
                             ('Bob', 1),
                             ('Joe', 1),
                             ('Smithers', 2)], columns=['name', 'count'])
    tm.assert_frame_equal(result, expected)


def test_simple_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st)
    result = into(pd.DataFrame, compute(expr, {st: sq, rt: rq}))
    expected = compute(expr, {st: sdf.reset_index(), rt: rdf.reset_index()})
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='Only inner join implemented for QTable')
def test_outer_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st, how='outer')
    result = compute(expr, {st: sq, rt: rq})
    expected = compute(expr, {st: sdf.reset_index(), rt: rdf.reset_index()})
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='Cannot specify different columns')
def test_different_column_join(rt, st, rq, sq, rdf, sdf):
    expr = bz.join(rt, st, on_left='name', on_right='alias')
    result = compute(expr, {st: sq, rt: rq})
    expected = compute(expr, {st: sdf.reset_index(), rt: rdf.reset_index()})
    tm.assert_frame_equal(result, expected)


def test_sort(t, q, df):
    expr = t.sort('name')
    result = compute(expr, q)
    expected = df.sort('name', kind='mergesort').reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


def test_multicolumn_sort(t, q, df):
    expr = t.sort(['name', 'amount'])
    result = compute(expr, q)
    expected = compute(expr, df).reset_index(drop=True)
    tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=ValueError,
                   reason='axis == 1 not supported on record types')
def test_nelements(t, q, df):  # pragma: no cover
    assert compute(t.nelements(axis=1), q) == df.shape[1]


def test_discover(q):
    assert (discover(q) ==
            dshape('var * {name: string, id: int64, amount: float64, '
                   '       when: datetime, on: date}'))


def test_into_from_keyed(rq, rdf):
    result = into(pd.DataFrame, rq)
    tm.assert_frame_equal(result, rdf)


def test_into_from_qtable(q, df):
    result = into(pd.DataFrame, q)
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
    assert compute(expr, q) == compute(expr, df)


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
    tm.assert_frame_equal(compute(expr, q),
                          compute(expr, df).reset_index(drop=True))


def test_neg_bounded_slice(t, q, df):
    # this should be empty in Q, though it's possible to do this
    expr = t[-2:5]
    tm.assert_frame_equal(compute(expr, q),
                          compute(expr, df).reset_index(drop=True))


def test_neg_bounded_by_negative_slice(t, q, df):
    # this should be empty in Q, though it's possible to do this
    expr = t[-5:-2]
    tm.assert_frame_equal(compute(expr, q),
                          compute(expr, df).reset_index(drop=True))


def test_raw_summary(t, q, df):
    expr = summary(s=t.amount.sum(), mn=t.id.mean())
    tm.assert_series_equal(compute(expr, q).squeeze(), compute(expr, df))


def test_simple_summary(t, q, df):
    expr = by(t.name, s=t.amount.sum())
    tm.assert_frame_equal(compute(expr, q), compute(expr, df))


def test_twofunc_summary(t, q, df):
    expr = by(t.name, s=t.amount.sum(), mn=t.id.mean())
    tm.assert_frame_equal(compute(expr, q), compute(expr, df))


def test_complex_summary(t, q, df):
    expr = by(t.name, s=t.amount.sum(), mn=t.id.mean(),
              mx=t.amount.max() + 1)
    tm.assert_frame_equal(compute(expr, q), compute(expr, df))


def test_distinct(t, q, df):
    expr = t.name.distinct()
    tm.assert_series_equal(compute(expr, q), compute(expr, df))


def test_nunique(t, q, df):
    expr = t.name.nunique()
    assert compute(expr, q) == compute(expr, df)


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
    tm.assert_frame_equal(compute(expr, q), compute(expr, df))


def test_by_name(t, q, df):
    expr = by(t.when.day, m=t.amount.mean())
    name = 'when_day'
    tm.assert_frame_equal(compute(expr, q),
                          compute(expr, df).rename(columns={'index': name}))


def test_by_with_complex_where(t, q, df):
    r = t[((t.amount > 1) & (t.id > 0)) | (t.amount < 4)]
    expr = by(r.name, s=r.amount.sum(), m=r.amount.mean())
    tm.assert_frame_equal(compute(expr, q), compute(expr, df))


@pytest.mark.parametrize(('d', 'ts'),
                         [('2014-01-02',
                           pd.Timestamp('2014-01-02 00:00:00.000000001')),
                          (pd.Timestamp('2014-01-02'),
                           '2014-01-02 00:00:00.000000001')])
def test_datelike_compare(date_t, date_q, date_df, d, ts):
    def compare(lhs, rhs):
        expr = date_t[lhs == rhs]
        result = compute(expr, date_q)
        expected = compute(expr, date_df).reset_index(drop=True)
        tm.assert_frame_equal(result, expected)

    compare(date_t.d, d)
    compare(d, date_t.d)
    compare(date_t.ts, ts)
    compare(ts, date_t.ts)


def test_timespan_discover(timespan_table):
    result = discover(timespan_table)
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
