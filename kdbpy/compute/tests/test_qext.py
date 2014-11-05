import pytest
import pandas as pd
import pandas.util.testing as tm
pytest.importorskip('blaze')
pytest.importorskip('datashape')
from datashape import dshape
from blaze import compute


def test_bar(t, q, df):
    expr = t.amount.bar(5)
    assert expr.fields == t.amount.fields
    assert expr.dshape == dshape('var * int64')
    assert expr.schema == dshape('int64')
    result = compute(expr, q)
    expected = df.amount - df.amount % 5
    tm.assert_series_equal(result, expected)


def test_bar_date(t, q, df):
    expr = t.when.date.bar(5)  # should be daily
    assert expr.fields == ['when_date']
    assert expr.dshape == dshape('var * date')
    assert expr.schema == dshape('date')

    result = compute(expr, q)
    result = result.order().reset_index(drop=True)

    assert result.name == 'when_date'

    expected = pd.Series([pd.Timestamp('2003-12-01'),
                          pd.Timestamp('2004-01-25'),
                          pd.Timestamp('2004-06-23'),
                          pd.Timestamp('2004-08-27'),
                          pd.Timestamp('2005-02-28'),
                          pd.Timestamp('2005-02-28')],
                         name='when_date')
    tm.assert_series_equal(result, expected)
