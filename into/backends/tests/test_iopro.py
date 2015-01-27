import sqlalchemy as sa
from into import into, resource, convert
from into.core import path
from into.backends.sql import dshape_to_table
from into.backends.iopro import (iopro_sqltable_to_ndarray,
                                 iopro_sqlselect_to_ndarray)
import numpy as np
from numpy import ndarray, random
from datashape import discover, dshape

url = 'mysql+pymysql://continuum:continuum_test@localhost/continuum'
sql_engine = resource(url)


def make_market_data(N=1000, randomize=True):
    """
    This function is intended to make a large amount of fake market data
    It will return a struct-array/rec-array that can be inserted into
      the db for later querying and processing
    """
    
    if randomize:
        low_multiplier = random.rand(N)
        high_multiplier = random.rand(N)+1
        
    nums = np.arange(N)
    
    arr = np.zeros(N, dtype=[('symbol_', 'S21'), ('open_', 'f4'),
                             ('low_', 'f4'), ('high_', 'f4'),
                             ('close_', 'f4'), ('volume_', 'int')])

    arr['symbol_'] = nums.astype(str)
    arr['open_'] = nums
    arr['low_'] = nums * low_multiplier
    arr['high_'] = nums * high_multiplier
    arr['close_'] = nums
    arr['volume_'] = nums

    return arr


def populate_db(sql_engine, tablename, array):

    try:
        table = sa.Table(tablename, sa.MetaData(sql_engine))
        table.drop()
    except:
        pass

    table = dshape_to_table(tablename, discover(array), sa.MetaData(sql_engine))
    table.create(sql_engine)
    into(table, array)

    return table


def test_iopro_sqltable_to_ndarray():

    tablename = "market"
    data = make_market_data(N=10000)
    table = populate_db(sql_engine, tablename, data)

    #convert() will automatically figure out how to
    #  go from a table to np.ndarray
    #convert should eventually call iopro_sqltable_to_ndarray
    res = convert(np.ndarray, table)
    res2 = iopro_sqltable_to_ndarray(table)
    
    assert all(res == res2)

def test_iopro_sqlselect_to_ndarray():

    tablename = "market"
    data = make_market_data(N=10000)
    table = populate_db(sql_engine, tablename, data)
    select = sa.sql.select([table.c.high_])

    #convert() will automatically figure out how to
    #  go from a table to np.ndarray
    #convert should eventually call iopro_sqlselect_to_ndarray
    res = convert(np.ndarray, select)
    res2 = iopro_sqlselect_to_ndarray(select)

    assert all(res == res2)


def test_iopro_convert_path():
    """The function used to go from sql->ndarray should be the iopro function

    Query the into.convert.graph path to make sure that
    """
    expected = [(sa.sql.selectable.Select, ndarray, iopro_sqlselect_to_ndarray)]
    result = path(convert.graph, sa.sql.Select, np.ndarray)

    assert expected == result
    
    expected = [(sa.Table, ndarray, iopro_sqltable_to_ndarray)]
    result = path(convert.graph, sa.Table, np.ndarray)

    assert expected == result
    
