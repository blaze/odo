from __future__ import absolute_import, print_function

import getpass
import sqlalchemy as sa
import numpy as np
from toolz import memoize

from ..convert import convert
# This import will fail if iopro is not installed.
# The goal is to simply have all iopro converters
#  be missing if iopro isn't around
import iopro
import iopro.pyodbc

port_defaults = {
    'mysql': 3306,
    'postgresql': 5432
}


db_defaults = {
    'postgresql': 'public'
}


@memoize
def get_iopro_db_engine(engine):
    """Return an iopro.pyodbc-ized sqlachemy engine

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        A SQLAlchemy engine

    Returns
    -------
    An new sqlalchemy engine that will use ``iopro.pyodbc`` to make the
    connection to the db. If the connection cannot be made, a
    NotImplementedError exception will be raised so that a calling
    "convert()" or "into()" will be able to use a different
    sql->ndarray conversion path.

    Notes
    -----
    This function will try to make a db connection that ``iopro.pyodbc`` can
    use from an existing sqlalchemy engine/connection.

    Here's an example connection string for a mysql connection::

    connectionstring = "DRIVER={mysql};\
                       SERVER=localhost;\
                       DATABASE=continuum_db;\
                       UID=continuum_user;\
                       PASSWORD=continuum_test;\
                       OPTION=3;"

    * The DRIVER={mysql} part comes from the odbcinst.ini file that *MUST*
      exist (on *nix) in order for ODBC to be able to find the correct
      driver.
    * Furthermore, this file should have sections that *MATCH* the
      sqlalchemy driver names. That is, if
      ``sqlalchemy.engine.get_backend_name()`` returns "mysql", then your
      odbcinst.ini file should have a section resembling this::

      [mysql]
      Description= MySQL driver
      Driver= /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so

    * An ``odbc.ini`` file is NOT required by this function.
    * My understanding is that odbc.ini is only required for pre-defined
      data-source names (DSNs).
    * No DSNs are used here because we extract all necessary data from the
      sqlalchemy engine.
    """
    url = engine.url
    driver = dialect = url.get_backend_name()
    connect_args = url.translate_connect_args()

    server = connect_args.get('host', 'localhost')
    uid = connect_args.get('username', getpass.getuser())
    password = connect_args.get('password', '')
    database = connect_args.get('database', db_defaults.get(driver, ''))
    port = connect_args.get('port', port_defaults.get(driver, ''))
    option = connect_args.get('option', 3 if driver == 'mysql' else '')

    if driver not in ('mysql', 'mssql'):
        raise NotImplementedError("sqlalchemy does not support the %s database"
                                  " with the pyodbc dialect yet." % driver)

    dialect = '%s+pyodbc' % driver

    # Note: It appears to be OK to leave parameters empty. Default
    #  values will be filled in by the odbc driver.
    connection_string = ("DRIVER=%s;"
                         "SERVER=%s;"
                         "PORT=%d;"
                         "DATABASE=%s;"
                         "UID=%s;"
                         "PASSWORD=%s;"
                         "OPTION=%s;" % (driver, server, port, database, uid,
                                         password, option))

    def getconn():
        return iopro.pyodbc.connect(connection_string)

    return sa.create_engine("%s://" % dialect, creator=getconn)


# The cost=26.0 comes from adding up all of the costs from the original
#  convert() path and dividing by 15 (15x is the new speedup)
@convert.register(np.ndarray, sa.sql.Select, cost=26.0)
def iopro_sqlselect_to_ndarray(sqlselect, **kwargs):
    """Return a numpy struct-array from a sql.Select

    Parameters
    ----------
    sqlselect : a sqlalchemy sql.Select object

    Return
    ------
    A numpy struct array with the sql results

    Notes
    -----
    In testing, using fetchsarray() on mysql is about
    15x faster than naive methods at the 10,000 row
    range.
    """

    # Remake the database engine so that iopro can use fetchsarray()
    orig_engine = sqlselect.bind
    iopro_db_engine = get_iopro_db_engine(orig_engine)
    conn = iopro_db_engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(str(sqlselect))
        return cursor.fetchsarray()
    finally:
        cursor.close()
        conn.close()


# The cost=26.0 comes from adding up all of the costs from the original
#  convert() path and dividing by 15 (15x is the new speedup)
@convert.register(np.ndarray, sa.Table, cost=26.0)
def iopro_sqltable_to_ndarray(table, **kwargs):
    """Return a numpy struct-array of an entire table

    Parameters
    ----------
    table : a sqlalchemy Table object

    Return
    ------
    A numpy struct array with the sql results
    """
    return iopro_sqlselect_to_ndarray(table.select())
