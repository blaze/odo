from into.convert import convert
from into.chunks import chunks
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


def test_iterator_and_numpy_chunks():
    c = convert(chunks(np.ndarray), [1, 2, 3])
    assert isinstance(c, chunks(np.ndarray))

    L = convert(list, c)
    assert L == [1, 2, 3]
