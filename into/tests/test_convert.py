from into.convert import convert
from datashape import discover
import numpy as np
import pandas as pd

def test_basic():
    assert convert(tuple, [1, 2, 3]) == (1, 2, 3)


def test_array_to_set():
    assert convert(set, np.array([1, 2, 3])) == set([1, 2, 3])


def eq(a, b):
    c = a == b
    if isinstance(c, (np.ndarray, pd.Series)):
        c = c.all()
    return c


def test_set_to_Series():
    assert eq(convert(pd.Series, set([1, 2, 3])),
              pd.Series([1, 2, 3]))


def test_Series_to_set():
    assert convert(set, pd.Series([1, 2, 3])) == set([1, 2, 3])


def test_dataframe_and_series():
    s = pd.Series([1, 2, 3], name='foo')
    df = convert(pd.DataFrame, s)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['foo']

    s2 = convert(pd.Series, df)
    assert isinstance(s2, pd.Series)

    assert s2.name == 'foo'


def test_convert_is_robust_to_failures():
    class A(object): pass
    class B(object): pass
    class C(object): pass
    discover.register((A, B, C))(lambda x: 'int')
    convert.register(B, A, cost=1.0)(lambda x, **kwargs: 1)
    convert.register(C, B, cost=1.0)(lambda x, **kwargs: x / 0) # note that this errs
    convert.register(C, A, cost=10.0)(lambda x, **kwargs: 2)

    assert convert(C, A()) == 2
