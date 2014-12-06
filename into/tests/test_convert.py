from into.convert import convert
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
