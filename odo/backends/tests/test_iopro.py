from __future__ import absolute_import, print_function

import pytest

sa = pytest.importorskip('sqlalchemy')
iopro = pytest.importorskip('iopro')
pytest.importorskip('pyodbc')

from odo import into, convert, drop
from odo.core import path
from odo.backends.sql import dshape_to_table
from odo.backends.iopro import (iopro_sqltable_to_ndarray,
                                iopro_sqlselect_to_ndarray)
import numpy as np
from numpy import ndarray, random, around
from datashape import discover
import getpass

import iopro.pyodbc

# WARNING: pyodbc has no problem connecting to postgres,
#  but sqlalchemy doesn't have a dialect to do so.
#  So, we can't actually use a postgres database in this "convert" testing


@pytest.fixture
def postgres():
    url = 'postgresql+psycopg2://postgres@localhost/test'
    try:
        engine = sa.create_engine(url)
        engine.connect()
    except sa.exc.OperationalError as e:
        pytest.skip("Cannot connect to postgres database with error: %s" % e)
    else:
        return engine


@pytest.fixture
def pyodbc_postgres():
    pyodbc_postgres_url = ('DRIVER={postgresql};SERVER=localhost;UID=postgres;'
                           'DATABASE=test;')
    try:
        return iopro.pyodbc.connect(pyodbc_postgres_url)
    except sa.exc.OperationalError as e:
        pytest.skip(
            "Cannot connect to postgres database using pyodbc with error: %s" %
            e)


@pytest.fixture
def mysql():
    try:
        engine = sa.create_engine('mysql+pymysql://{0}@localhost:3306/test'
                                  ''.format(getpass.getuser()))
        engine.connect()
    except sa.exc.OperationalError as e:
        pytest.skip("Cannot connect to postgres database with error: %s" % e)
    else:
        return engine


@pytest.fixture
def pyodbc_mysql():
    pyodbc_mysql_url = ('DRIVER={mysql};'
                        'SERVER=localhost;'
                        'DATABASE=test;'
                        'UID=%s;'
                        'PORT=3306;'
                        'OPTION=3;' % getpass.getuser())
    try:
        return iopro.pyodbc.connect(pyodbc_mysql_url)
    except (sa.exc.OperationalError, iopro.pyodbc.Error) as e:
        pytest.skip("Cannot connect to mysql database using pyodbc with error:"
                    " %s" % e)


def make_market_data(n):
    """Make some fake market data"""
    low = random.rand(n)
    high = random.rand(n) + 1

    nums = np.arange(n)
    syms = np.random.choice(['AAPL', 'GOOG', 'MSFT', 'FB', 'AMZN'], size=n)

    arr = np.rec.fromarrays([syms,
                             nums,
                             around(nums * low, decimals=2),
                             around(nums * high, decimals=2),
                             nums,
                             nums],
                            names='symbol,open,low,high,close,volume')

    return arr


def populate_db(sql_engine, tablename, array):
    drop(sa.Table(tablename, sa.MetaData(sql_engine)))
    table = dshape_to_table(
        tablename, discover(array), sa.MetaData(sql_engine))
    table.create(sql_engine)
    return into(table, array)


def test_pyodbc_mysql_connection(pyodbc_mysql):
    cursor = pyodbc_mysql.cursor()
    cursor.execute("show tables")
    cursor.fetchall()
    cursor.close()
    pyodbc_mysql.close()


def test_pyodbc_postgresql_connection(pyodbc_postgres):
    cursor = pyodbc_postgres.cursor()
    cursor.execute(
        "select tablename from pg_tables where schemaname = 'public'")
    cursor.fetchall()
    cursor.close()
    pyodbc_postgres.close()


def test_iopro_convert_path():
    """The function used to go from sql->ndarray should be the iopro function

    Query the into.convert.graph path to make sure that
    """
    expected = [
        (sa.sql.selectable.Select, ndarray, iopro_sqlselect_to_ndarray)]
    result = path(convert.graph, sa.sql.Select, np.ndarray)

    assert expected == result

    expected = [(sa.Table, ndarray, iopro_sqltable_to_ndarray)]
    result = path(convert.graph, sa.Table, np.ndarray)

    assert expected == result


# This function shouldn't be run by pytest.
# It should be run by other functions
def iopro_sqltable_to_ndarray_example(sql_engine):

    tablename = "market"
    data = make_market_data(10)
    table = populate_db(sql_engine, tablename, data)

    # convert() will automatically figure out how to
    #  go from a table to np.ndarray
    # convert should eventually call iopro_sqltable_to_ndarray
    res = convert(np.ndarray, table, dshape=discover(table))
    res2 = iopro_sqltable_to_ndarray(table)
    res3 = into(np.ndarray, table)

    drop(table)

    np.testing.assert_array_equal(res, res2)
    np.testing.assert_array_equal(res2, res3)

    for name in ('low', 'high', 'open', 'close'):
        assert np.allclose(res2[name], data[name])


def iopro_sqlselect_to_ndarray_example(sql_engine):
    tablename = "market"
    data = make_market_data(10)
    table = populate_db(sql_engine, tablename, data)
    select = sa.sql.select([table.c.high])

    # convert() will automatically figure out how to
    #  go from a table to np.ndarray
    # convert should eventually call iopro_sqlselect_to_ndarray
    res = convert(np.ndarray, select, dshape=discover(select))
    res2 = iopro_sqlselect_to_ndarray(select)
    res3 = into(np.ndarray, select)

    drop(table)

    assert all(res == res2)
    assert all(res2 == res3)
    assert np.allclose(res2['high'], data['high'])


def test_iopro_mysql(mysql):
    iopro_sqltable_to_ndarray_example(mysql)
    iopro_sqlselect_to_ndarray_example(mysql)


@pytest.mark.xfail(not hasattr(sa.dialects.postgresql, 'pyodbc'),
                   reason="There's no postgresql+pyodbc driver for SQLAlchemy")
def test_iopro_postgresql(postgres):
    iopro_sqltable_to_ndarray_example(postgres)
    iopro_sqlselect_to_ndarray_example(postgres)
