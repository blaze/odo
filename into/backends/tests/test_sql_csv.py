from __future__ import absolute_import, division, print_function

import pytest
import os

import pandas as pd
import datashape

from into.backends.sql_csv import append_csv_to_sql_table, copy_command
from into import resource, into, CSV, discover
from into.utils import tmpfile, ignoring


def normalize(s):
    s2 = ' '.join(s.strip().split()).lower().replace('_', '')
    return s2


fn = os.path.abspath('myfile.csv')
if os.name == 'nt':
    escaped_fn = fn.replace('\\', '\\\\')
else:
    escaped_fn = fn

csv = CSV(fn, delimiter=',', has_header=True)
ds = datashape.dshape('var * {name: string, amount: int}')
tbl = resource('sqlite:///:memory:::my_table', dshape=ds)


def test_postgres_load():
    assert normalize(copy_command('postgresql', tbl, csv)) == normalize("""
    COPY my_table from '%s'
        (FORMAT csv,
         DELIMITER E',',
         NULL '',
         QUOTE '"',
         ESCAPE '\\',
         HEADER True,
         ENCODING 'utf-8');
    """ % escaped_fn)


def test_mysql_load():
    assert normalize(copy_command('mysql', tbl, csv)) == normalize("""
            LOAD DATA  INFILE '%s'
            INTO TABLE my_table
            CHARACTER SET utf-8
            FIELDS
                TERMINATED BY ','
                ENCLOSED BY '"'
                ESCAPED BY '\\'
            LINES TERMINATED BY '\\n\\r'
            IGNORE 1 LINES;""" % escaped_fn)


def test_into_sqlite():
    data = [('Alice', 100), ('Bob', 200)]
    ds = datashape.dshape('var * {name: string, amount: int}')

    with tmpfile('.db') as dbpath:
        with tmpfile('.csv') as csvpath:
            csv = into(csvpath, data, dshape=ds, has_header=False)
            sql = resource('sqlite:///%s::mytable' % dbpath, dshape=ds)
            with ignoring(NotImplementedError):
                append_csv_to_sql_table(sql, csv)
                assert into(list, sql) == data


def test_into_sqlite_with_header():
    df = pd.DataFrame([('Alice', 100), ('Bob', 200)],
                      columns=['name', 'amount'])
    with tmpfile('.csv') as fn:
        csv = into(fn, df)

        with tmpfile('.db') as sql:
            db = resource('sqlite:///%s::df' % sql, dshape=discover(csv))
            result = into(db, csv)

            assert into(list, result) == into(list, df)


def test_into_sqlite_with_header_and_different_sep():
    df = pd.DataFrame([('Alice', 100), ('Bob', 200)],
                      columns=['name', 'amount'])
    with tmpfile('.csv') as fn:
        csv = into(fn, df, delimiter='|')

        with tmpfile('.db') as sql:
            db = resource('sqlite:///%s::df' % sql, dshape=discover(csv))
            result = into(db, csv)

            assert into(list, result) == into(list, df)


def test_into_sqlite_with_different_sep():
    df = pd.DataFrame([('Alice', 100), ('Bob', 200)],
                      columns=['name', 'amount'])
    with tmpfile('.csv') as fn:
        # TODO: get the  header  argument to work in into(CSV, other)
        df.to_csv(fn, sep='|', header=False, index=False)
        csv = CSV(fn, delimiter='|', has_header=False)

        with tmpfile('.db') as sql:
            db = resource('sqlite:///%s::df' % sql, dshape=discover(csv))
            result = into(db, csv)

            assert into(list, result) == into(list, df)
