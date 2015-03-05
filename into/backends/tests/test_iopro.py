from __future__ import absolute_import, print_function

import pytest

import subprocess

#skip_mysql will decorate functions that needs mysql to run correctly
ps = subprocess.Popen("ps aux | grep '[m]ysql'", shell=True,
                      stdout=subprocess.PIPE)
output = ps.stdout.read()
num_processes = len(output.splitlines())
skip_missing_mysql = pytest.mark.skipif(num_processes < 1,
                                        reason="MySQL not running")
#skip_postgresql will decorate functions that need postgres to run
ps = subprocess.Popen("ps aux | grep '[p]ostgres'", shell=True,
                      stdout=subprocess.PIPE)
output = ps.stdout.read()
num_processes = len(output.splitlines())
skip_missing_postgresql = pytest.mark.skipif(num_processes < 10,
                                     reason = "PostgreSQL not running")

import sqlalchemy as sa
from into import into, resource, convert
from into.core import path
from into.backends.sql import dshape_to_table
from into.backends.iopro import (iopro_sqltable_to_ndarray,
                                 iopro_sqlselect_to_ndarray)
import numpy as np
from numpy import ndarray, random, around
from datashape import discover, dshape
import getpass

import iopro, iopro.pyodbc

username = getpass.getuser()
mysql_url = 'mysql+pymysql://{0}@localhost:3306/test'.format(username)
mysql_url = 'mysql+pymysql://continuum:continuum_test@localhost:3306/continuum'.format(username)
pyodbc_mysql_url = 'DRIVER={{mysql}};SERVER=localhost;DATABASE=test;'\
                   'UID={};OPTION=3;'.format(username)
pyodbc_mysql_url = 'DRIVER={{mysql}};SERVER=localhost;DATABASE=continuum;'\
                   'UID={};PASSWORD={};OPTION=3;'.format("continuum", "continuum_test")

#WARNING: pyodbc has no problem connecting to postgres,
#  but sqlalchemy doesn't have a dialect to do so.
#  So, we can't actually use a postgres database in this "convert" testing
postgres_url = 'postgresql://postgres:postgres@localhost'
pyodbc_postgres_url = 'DRIVER={postgresql};SERVER=localhost;'\
                      'UID=postgres;PASSWORD=postgres;PORT=;OPTION=;'


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
    arr['low_'] = around(nums * low_multiplier, decimals=2)
    arr['high_'] = around(nums * high_multiplier, decimals=2)
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


def drop_table(table):
    """drop the sa.Table table

    This assumes that the table already exists.
    Perhaps because populate_db() created it not long ago.
    """
    try:
        table.drop()
    except:
        pass


@skip_missing_mysql
def test_pyodbc_mysql_connection():
    odbc = iopro.pyodbc.connect(pyodbc_mysql_url)
    cursor = odbc.cursor()
    cursor.execute("show tables")
    res = cursor.fetchall()
    cursor.close()
    odbc.close()


@skip_missing_postgresql
def test_pyodbc_postgresql_connection():
    odbc = iopro.pyodbc.connect(pyodbc_postgres_url)
    cursor = odbc.cursor()
    cursor.execute("select tablename from pg_tables where schemaname = 'public'")
    res = cursor.fetchall()
    cursor.close()
    odbc.close()


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


#This function shouldn't be run by pytest.
#It should be run by other functions
def iopro_sqltable_to_ndarray_example(sql_engine):

    tablename = "market"
    data = make_market_data(N=10000)
    table = populate_db(sql_engine, tablename, data)

    #convert() will automatically figure out how to
    #  go from a table to np.ndarray
    #convert should eventually call iopro_sqltable_to_ndarray
    res = convert(np.ndarray, table, dshape=discover(table))
    res2 = iopro_sqltable_to_ndarray(table)
    res3 = into(np.ndarray, table)

    drop_table(table)

    assert all(res == res2)
    assert all(res2 == res3)
    assert np.allclose(res2['low_'], data['low_'])
    assert np.allclose(res2['high_'], data['high_'])
    assert np.allclose(res2['open_'], data['open_'])
    assert np.allclose(res2['close_'], data['close_'])


#This function shouldn't be run by pytest.
#It should be run by other functions
def iopro_sqlselect_to_ndarray_example(sql_engine):

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

    drop_table(table)

    assert all(res == res2)
    assert all(res2 == res3)
    assert np.allclose(res2['high_'], data['high_'])


@skip_missing_mysql
def test_iopro_mysql():
    sql_engine = sa.create_engine(mysql_url)

    iopro_sqltable_to_ndarray_example(sql_engine)
    iopro_sqlselect_to_ndarray_example(sql_engine)


@skip_missing_postgresql
def test_iopro_postgresql():
    sql_engine = sa.create_engine(postgres_url)

    iopro_sqltable_to_ndarray_example(sql_engine)
    iopro_sqlselect_to_ndarray_example(sql_engine)

