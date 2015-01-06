# tests fixtures

import pytest
import numpy as np
from pandas import DataFrame, date_range
from into import discover

@pytest.fixture(scope='module')
def df():
    return DataFrame({ 'id' : range(5),
                       'name' : ['Alice','Bob','Charlie','Denis','Edith'],
                       'amount' : [100,-200,300,400,-500] })
@pytest.fixture(scope='module')
def df2(df):
    df2 = df.copy()
    df2['date'] = date_range('20130101 09:00:00',freq='s',periods=len(df))
    return df2

@pytest.fixture(scope='module')
def arr():
    return np.array([(1, 'Alice', 100),
                     (2, 'Bob', -200),
                     (3, 'Charlie', 300),
                     (4, 'Denis', 400),
                     (5, 'Edith', -500)],
                    dtype=[('id', '<i8'), ('name', 'S7'), ('amount', '<i8')])

@pytest.fixture(scope='module')
def arr_dshape(arr):
    return discover(arr)

@pytest.fixture(scope='module')
def arr2():
    return np.ones((2, 3), dtype='i4')

@pytest.fixture(scope='module')
def bank():
    return ({'name': 'Alice', 'amount': 100},
            {'name': 'Alice', 'amount': 200},
            {'name': 'Bob', 'amount': 100},
            {'name': 'Bob', 'amount': 200},
            {'name': 'Bob', 'amount': 300})

def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c
