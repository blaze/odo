import pytest
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
