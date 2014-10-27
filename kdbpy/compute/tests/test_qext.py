import pandas.util.testing as tm
from blaze import compute


def test_bar(t, q, df):
    expr = t.amount.bar(5)
    result = compute(expr, q)
    expected = df.amount - df.amount % 5
    tm.assert_series_equal(result, expected)
