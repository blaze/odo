import sqlalchemy as sa
from into import into, resource, convert
from into.core import path
from into.backends.sql import dshape_to_table
from into.backends.iopro import (iopro_sqltable_to_ndarray,
                                 iopro_sqlselect_to_ndarray)
import numpy as np
from numpy import ndarray, random
from datashape import discover, dshape

import iopro, iopro.pyodbc

mysql_url = 'mysql+pymysql://continuum:continuum_test@localhost/continuum'
pyodbc_mysql_url = 'DRIVER={mysql};SERVER=localhost;DATABASE=continuum;'\
                   'UID=continuum;PASSWORD=continuum_test;OPTION=3;'

#WARNING: pyodbc has no problem connecting to postgres,
#  but sqlalchemy doesn't have a dialect to do so.
#  So, we can't actually use a postgres database in this "convert" testing
postgres_url = 'postgresql://continuum:continuum_test@localhost/continuum'
pyodbc_postgres_url = 'DRIVER={postgresql};SERVER=localhost;DATABASE=continuum;'\
                      'UID=continuum;PASSWORD=continuum_test;PORT=;OPTION=;'

sql_engine = resource(mysql_url)

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


def test_pyodbc_connections():
    odbc = iopro.pyodbc.connect(pyodbc_mysql_url)
    cursor = odbc.cursor()
    cursor.execute("show tables")
    res = cursor.fetchall()
    cursor.close()
    odbc.close()

    odbc = iopro.pyodbc.connect(pyodbc_postgres_url)
    cursor = odbc.cursor()
    cursor.execute("select tablename from pg_tables where schemaname = 'public'")
    res = cursor.fetchall()
    cursor.close()


def test_iopro_sqltable_to_ndarray():

    tablename = "market"
    data = make_market_data(N=10000)
    table = populate_db(sql_engine, tablename, data)

    #convert() will automatically figure out how to
    #  go from a table to np.ndarray
    #convert should eventually call iopro_sqltable_to_ndarray
    res = convert(np.ndarray, table, dshape=discover(table))
    res2 = iopro_sqltable_to_ndarray(table)
    res3 = into(np.ndarray, table)

    assert all(res == res2)
    assert all(res2 == res3)

def test_iopro_sqlselect_to_ndarray():

    tablename = "market"
    data = make_market_data(N=10000)
    table = populate_db(sql_engine, tablename, data)
    select = sa.sql.select([table.c.high_])

    #convert() will automatically figure out how to
    #  go from a table to np.ndarray
    #convert should eventually call iopro_sqlselect_to_ndarray
    res = convert(np.ndarray, select, dshape=discover(select))
    res2 = iopro_sqlselect_to_ndarray(select)
    res3 = into(np.ndarray, select)

    assert all(res == res2)
    assert all(res2 == res3)


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
