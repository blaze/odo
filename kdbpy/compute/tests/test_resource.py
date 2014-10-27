import pytest
import os

import shutil

import pandas as pd
import pandas.util.testing as tm
import blaze as bz
from toolz import first
from blaze import Data, by, into, compute
from blaze.compute.core import swap_resources_into_scope
from kdbpy.kdb import KQ, get_credentials


@pytest.fixture
def rstring():
    return 'kdb://pcloud@localhost:5000'


@pytest.fixture
def tstring(rstring):
    return rstring + '::t'


@pytest.yield_fixture(scope='module')
def kdb():
    kq = KQ(get_credentials(), start='restart')
    kq.eval(r'\l buildhdb.q')
    kq.eval(r'\l %s' % os.path.join('start', 'db'))
    yield kq
    kq.stop()
    shutil.rmtree(os.path.abspath('start'))


@pytest.fixture
def daily(rstring, kdb):
    return rstring + '/start/db::daily'


def test_resource(daily, kdb):
    data = Data(daily, engine=kdb)
    assert repr(data)


def test_field(daily, kdb):
    data = Data(daily, engine=kdb)
    qresult = data.price
    expr, data = swap_resources_into_scope(qresult, {})
    expected = compute(expr, first(data.values()))
    result = into(pd.Series, qresult)
    assert result.name == expected.name
    tm.assert_series_equal(result, expected)


def test_simple_op(daily, kdb):
    data = Data(daily, engine=kdb)
    qresult = data.price + 1
    result = into(pd.DataFrame, qresult)
    expr, data = swap_resources_into_scope(qresult, {})
    expected = into(pd.DataFrame, compute(expr, data))
    tm.assert_frame_equal(result, expected)


def test_complex_op_repr(daily, kdb):
    daily = Data(daily, engine=kdb)
    daily = bz.Symbol('daily', daily.dshape)
    result = by(daily.date.month,
                cnt=daily.price.nrows(),
                size=daily.size.sum(),
                wprice=(daily.price * daily.size).sum() / daily.price.count()
                )
    assert repr(result)


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='Figure out DateTime issues for By expressions')
def test_complex_op(daily, kdb):
    daily = Data(daily, engine=kdb)
    result = by(daily.date.month,
                cnt=daily.price.nrows(),
                size=daily.size.sum(),
                wprice=(daily.price * daily.size).sum() / daily.price.count())
    assert repr(result)


def test_complex_nondate_op(daily, kdb):
    # q) select cnt: count price, size: sum size, wprice: size wavg price
    #       by sym from daily
    daily = Data(daily, engine=kdb)
    qresult = by(daily.sym,
                 cnt=daily.price.nrows(),
                 size=daily.size.sum(),
                 wprice=(daily.price * daily.size).sum() / daily.price.sum())
    result = into(pd.DataFrame, qresult)
    expr, data = swap_resources_into_scope(qresult, {})
    expected = compute(expr, daily.data)
    tm.assert_frame_equal(result, expected)
