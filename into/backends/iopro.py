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
def get_iopro_db_engine(engine):
    """Return an iopro.pyodbc-ized sqlachemy engine

    Parameters
    ----------
    engine - A sqlalchemy engine

    Return
    ------
    An new sqlalchemy engine that will use iopro.pyodbc to make the
    connection to the db. If the connection cannot be made, a
    NotImplementedError exception will be raised so that a calling
    "convert()" or "into()" will be able to use a different
    sql->ndarray conversion path.
    
    Notes
    -----
    This function will try to make a db connection
      that iopro.pyodbc can use from an existing
      sqlalchemy engine/connection.
    Example connection string for a mysql connection:
    connectionstring = "DRIVER={mysql};\
                       SERVER=localhost;\
                       DATABASE=continuum_db;\
                       UID=continuum_user;\
                       PASSWORD=continuum_test;\
                       OPTION=3;"

    The DRIVER={mysql} part comes from the odbcinst.ini file that *MUST*
      exist (on *nix) in order for ODBC to be able to find the correct
      driver.
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
    url = engine.url
    connect_args = url.translate_connect_args()

    server = connect_args.get('host','')
    uid = connect_args.get('username','')
    password = connect_args.get('password','')
    database = connect_args.get('database','')
    port = connect_args.get('port','')
    option = connect_args.get('option','')

    driver = dialect = url.get_backend_name()
    
    if driver == "mysql":
        #option=3 is required by mysql
        option = "3"
        dialect = "mysql+pyodbc"
    elif driver == "mssql":
        dialect = "mssql+pyodbc"
    else:
        raise NotImplementedError("sqlalchemy does not support the {} database "\
                                  "with the pyodbc dialect yet.".format(driver))
        
    #Note: It appears to be OK to leave parameters empty. Default
    #  values will be filled in by the odbc driver.
    connection_string = "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};"\
                        "PASSWORD={};OPTION={};".format(driver, server, port,
                                                        database, uid, password,
                                                        option)

    def getconn():
        #If the odbc connection cannot be made, raising the
        #  NotImplementedError exception should make the convert
        #  dispatch manager fall back to a working conversion path.
        try:
            iopro_db_conn = iopro.pyodbc.connect(connection_string)
        except Exception as e:
            raise NotImplementedError("iopro odbc connection failed. "
                                      "Falling back to defaults. "
                                      "Original exception: %s" % (e))
        return iopro_db_conn

    new_sa_engine = sa.create_engine("{}://".format(dialect), creator=getconn)

    return new_sa_engine

    
#The cost=26.0 comes from adding up all of the costs from the original
#  convert() path and dividing by 15 (15x is the new speedup)
@convert.register(np.ndarray, sa.sql.Select, cost=26.0)
def iopro_sqlselect_to_ndarray(sqlselect, **kwargs):
    """Return a numpy struct-array from a sql.Select

    Parameters
    ----------
    sqlselect - a sqlalchemy sql.Select object

    Return
    ------
    A numpy struct array with the sql results

    Notes
    -----
    In testing, using fetchsarray() on mysql is about
    15x faster than naive methods at the 10,000 row
    range.
    """

    #Remake the database engine so that iopro can use fetchsarray()
    orig_engine = sqlselect.bind
    iopro_db_engine = get_iopro_db_engine(orig_engine)

    conn = iopro_db_engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(str(sqlselect))
    results = cursor.fetchsarray()
    cursor.close()
    conn.close()

    return results


#The cost=26.0 comes from adding up all of the costs from the original
#  convert() path and dividing by 15 (15x is the new speedup)
@convert.register(np.ndarray, sa.Table, cost=26.0)
def iopro_sqltable_to_ndarray(table, **kwargs):
    """Return a numpy struct-array of an entire table

    Parameters
    ----------
    table - a sqlalchemy Table object

    Return
    ------
    A numpy struct array with the sql results
    """
    
    results = iopro_sqlselect_to_ndarray(sa.sql.select([table]))

    return results

