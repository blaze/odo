# tests fixtures

import pytest
from pandas import DataFrame, date_range

@pytest.fixture
def df():
    return DataFrame({ 'id' : range(5),
                       'name' : ['Alice','Bob','Charlie','Denis','Edith'],
                       'amount' : [100,-200,300,400,-500] })
@pytest.fixture
def df2(df):
    df2 = df.copy()
    df2['date'] = date_range('20130101 09:00:00',freq='s',periods=len(df))
    return df2
