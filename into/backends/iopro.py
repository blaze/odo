from __future__ import absolute_import, print_function

import sqlalchemy as sa
import numpy as np
from toolz import memoize

from ..convert import convert
#This import will fail if iopro is not installed.
#The goal is to simply have all iopro converters
#  be missing if iopro isn't around
import iopro
import iopro.pyodbc

@memoize
def get_iopro_db_engine(raw_sa_conn):
    """Return a memoized pyodbc connection that iopro can use.
    
    This function will try to make a db connection
      that iopro.pyodbc can use from an existing
      sqlalchemy engine/connection.
    Example connection string for a mysql connection:
    connectionstring = "DRIVER={mysql};\
                       SERVER=localhost;\
                       DATABASE=continuum_db;\
                       USER=continuum_user;\
                       PASSWORD=continuum_test;\
                       OPTION=3;"

    The DRIVER={mysql} part comes from the odbcinst.ini file that *MUST*
      exist in order for ODBC to be able to find the correct driver.
    Furthermore, this file should have sections that *MATCH* the
      sqlalchemy driver names.
    i.e. if sqlalchemy.engine.get_backend_name() returns "mysql",
      then your odbcinst.ini file should have a section resembling this:
      [mysql]
      Description= MySQL driver
      Driver= /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so

    An odbc.ini file is NOT required by this function.
    My understanding is that odbc.ini is only required for pre-defined
      data-source names (DSNs).
    No DSNs are used here because we extract all necessary data from
      the sqlalchemy engine.
                       
    """
    url = raw_sa_conn.url
    connect_args = url.translate_connect_args()

    server = connect_args.get('host')
    user = connect_args.get('username')
    password = connect_args.get('password')
    database = connect_args.get('database')
    port = connect_args.get('port')

    driver = dialect = url.get_backend_name()
    
    if driver == "mysql":
        #option 3 is required by mysql
        option = "3"
        dialect = "mysql+pyodbc"

    #Note: It appears to be OK to leave parameters empty. Default
    #  values will be filled in by the odbc driver.
    connection_string = "DRIVER={{{}}};\
    SERVER={};\
    PORT={};\
    DATABASE={};\
    USER={};\
    PASSWORD={};\
    OPTION={};\
    ".format(driver, server, port, database, user, password, option)

    def getconn():
        #If the odbc connection cannot be made, raising the
        #  NotImplementedError exception should make the convert
        #  dispatch manager fall back to a working conversion path.
        try:
            iopro_db_conn = iopro.pyodbc.connect(connection_string)
        except:
            raise NotImplementedError("iopro odbc connection failed. "
                                      "Falling back to defaults.")
        return iopro_db_conn

    new_sa_engine = sa.create_engine("{}://".format(dialect), creator=getconn)

    return new_sa_engine
    
@convert.register(np.ndarray, sa.sql.Select, cost=10.0)
def iopro_sqlselect_to_ndarray(sqlselect, **kwargs):
    """Use iopro to dump a sql select to an ndarray *fast*

    Remake the database connection so that iopro can
    use its fast fetchsarray() function to turn sql
    data into a numpy struct array (not a recarray).
    In testing, using fetchsarray() on mysql is about
    15x faster than naive methods at the 10,000 row
    range.

    If a pyodbc database connection can't be made, get_iopro_db_engine
    will raise a NotImplementedError thereby letting the
    convert() dispatcher fallback to a different conversion
    """

    orig_engine = sqlselect.bind
    iopro_db_engine = get_iopro_db_engine(orig_engine)

    conn = iopro_db_engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(str(sqlselect))
    results = cursor.fetchsarray()
    cursor.close()
    conn.close()

    return results

@convert.register(np.ndarray, sa.Table, cost=10.0)
def iopro_sqltable_to_ndarray(table, **kwargs):
    """Use iopro to dump a sql table to an ndarray *fast*

    Remake the database connection so that iopro can
    use its fast fetchsarray() function to turn sql
    data into a numpy struct array (not a recarray).
    In testing, using fetchsarray() on mysql is about
    15x faster than naive methods at the 10,000 row
    range. 

    If a pyodbc database connection can't be made, get_iopro_db_engine
    will raise a NotImplementedError thereby letting the
    convert() dispatcher fallback to a different conversion
    """
    
    orig_engine = table.bind
    iopro_db_engine = get_iopro_db_engine(orig_engine)

    conn = iopro_db_engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(str(sa.sql.select([table])))
    results = cursor.fetchsarray()
    cursor.close()
    conn.close()

    return results

