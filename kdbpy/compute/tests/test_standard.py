from __future__ import print_function

import pytest

import math
import numpy as np
import pandas as pd
import pandas.util.testing as tm

from into import into

import blaze as bz
from blaze import by, compute

from kdbpy.compute.qtable import is_standard
from kdbpy.compute.functions import wmean
from kdbpy.tests import assert_series_equal


def test_resource_doesnt_bork(par):
    assert repr(par.daily)


def test_field(par):
    expr = par.daily.price
    expected = par.data.eval('select price from daily').squeeze()
    result = compute(expr)
    assert result.name == expected.name
    tm.assert_series_equal(result, expected)


def test_field_name(par):
    result = par.daily.price
    names = repr(result).split('\n')[0].strip().split()
    assert len(names) == 1
    assert names[0] == 'price'


def test_simple_op(par):
    expr = par.daily.price + 1
    result = compute(expr)
    expected = par.data.eval('select price + 1 from daily').squeeze()
    tm.assert_series_equal(result, expected)


def test_complex_date_op_repr(par):
    sym = bz.symbol('daily', par.daily.dshape)
    result = by(sym.date.month,
                cnt=sym.nrows,
                size=sym.size.sum(),
                wprice=wmean(sym.price, weights=sym.size))
    assert repr(result)


def test_complex_date_op(par):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by date from daily
    expr = by(par.daily.date,
              cnt=par.daily.price.count(),
              size=par.daily.size.sum(),
              wprice=wmean(par.daily.price, weights=par.daily.size))
    assert repr(expr)
    result = compute(expr)
    query = ('select cnt: count price, size: sum size, wprice: size wavg price'
             '  by date from daily')
    expected = par.data.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_complex_nondate_op(par):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by sym from daily
    daily = par.daily
    expr = by(daily.sym,
              cnt=daily.price.count(),
              size=daily.size.sum(),
              wprice=wmean(daily.price, weights=daily.size))
    assert repr(expr)
    result = compute(expr)
    query = ('select cnt: count price, size: sum size, wprice: size wavg price'
             ' by sym from daily')
    expected = par.data.eval(query).reset_index()
    tm.assert_frame_equal(result, expected)


def test_is_standard(par):
    assert is_standard(par.daily)


def test_by_mean(par):
    expr = by(par.daily.sym, price=par.daily.price.mean())
    qs = 'select avg price by sym from daily'
    expected = par.data.eval(qs).reset_index()
    result = compute(expr)
    tm.assert_frame_equal(result, expected)


def test_sum_after_subset(par):
    r = par.daily[(par.daily.date == par.daily.date[-1]) &
                  (par.daily.sym == 'IBM')]
    result = compute(r.price.sum())
    expected = into(pd.Series, r.price).sum()
    assert result == expected


def test_nrows(par):
    assert compute(par.daily.nrows) == compute(par.daily.date.nrows)


def test_nunique(par):
    expr = par.daily.sym.nunique()
    result = compute(expr)
    expected = par.data.eval('count distinct exec sym from daily')
    assert result == expected


def test_dateattr_nrows(par):
    assert compute(par.daily.nrows) == compute(par.daily.date.day.nrows)


@pytest.mark.xfail(raises=AttributeError,
                   reason="'Field' object has no attribute 'name'")
def test_foreign_key(db):
    expr = db.market.ex_id.name
    result = compute(expr)
    expected = db.data.eval('select ex_id.name from market').squeeze()
    assert_series_equal(result, expected)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='bad type for grouper in by expression')
def test_by_grouper_type_fails(par):
    expr = by(par.daily.sym[::2], avg_price=par.daily.price.mean())
    query = 'select avg_price: avg price by sym from daily where (i mod 2) = 0'
    expected = par.data.eval(query).reset_index()
    result = compute(expr)
    tm.assert_frame_equal(result, expected)


def test_convert_qtable_to_frame(db, q):
    tablename = q.tablename
    expected = db.data.eval(tablename)
    result = into(pd.DataFrame, getattr(db, tablename))
    tm.assert_frame_equal(result, expected)


def test_append_frame_to_in_memory_qtable(db, q):
    df = db.data.eval(q.tablename)
    expected = pd.concat([df, df], ignore_index=True)
    result = into(pd.DataFrame, into(q, df))
    tm.assert_frame_equal(result, expected)


def test_append_with_different_columns(db, q):
    df = db.data.eval(q.tablename)
    old_df = df.copy()
    df['foobarbaz'] = np.arange(len(df))
    expected = pd.concat([old_df, old_df], ignore_index=True)
    result = into(pd.DataFrame, into(q, df))
    tm.assert_frame_equal(result, expected)


def test_append_with_kq(db, q):
    df = db.data.eval(q.tablename)
    expected = pd.concat([df, df], ignore_index=True)
    result = into(pd.DataFrame, into(compute(db.t), df))
    tm.assert_frame_equal(result, expected)


def test_multi_groupby(par):
    t = par.daily
    qs = ('select price: sum price, '
          '       fst: first date, '
          '       lst: last date, '
          '       cnt: count date '
          '   by sym from daily')
    expected = par.data.eval(qs).reset_index()
    expr = by(t.sym,
              price=t.price.sum(),
              fst=t.date.min(),
              lst=t.date.max(),
              cnt=t.date.nrows)
    result = compute(expr)
    tm.assert_frame_equal(result.sort_index(axis=1),
                          expected.sort_index(axis=1))


def test_abs(par):
    expr = bz.expr.math.abs(-par.daily.price)
    result = compute(expr)
    expected = par.data.eval('abs neg daily.price')
    tm.assert_series_equal(result, expected)


missing_in_q = set(['sinh', 'cosh', 'tanh', 'asinh', 'acosh', 'atanh',
                    'isnan', 'degrees', 'expm1', 'trunc', 'radians',
                    'log1p'])


mapping = {
    'ceil': 'ceiling',
    'log10': '.kdbpy.log10'
}


@pytest.mark.parametrize('func',
                         filter(lambda x: (x not in missing_in_q and
                                           not x.startswith('__')),
                                set(dir(math)) & set(dir(bz.expr.math))))
def test_math(db, func):
    name = func
    f = getattr(bz, func)
    expr = f(db.daily.price)
    result = compute(expr)
    expected = db.data.eval('%s daily.price' % mapping.get(name, name))
    tm.assert_series_equal(result, expected)
