from __future__ import absolute_import, division, print_function

import os

import pytest
pytest.importorskip('sqlalchemy')

import pandas as pd
import datashape

from odo.backends.sql_csv import append_csv_to_sql_table, copy_command
from odo import resource, into, CSV, discover
from odo.utils import tmpfile, ignoring


def normalize(s):
    return ' '.join(s.strip().split()).lower().replace('_', '')


fn = os.path.abspath('myfile.csv')
escaped_fn = fn.encode('unicode_escape').decode() if os.name == 'nt' else fn

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
    result = normalize(copy_command('mysql', tbl, csv))
    expected = normalize("""
            LOAD DATA  INFILE '%s'
            INTO TABLE my_table
            CHARACTER SET utf8
            FIELDS
                TERMINATED BY ','
                ENCLOSED BY '"'
                ESCAPED BY '\\\\'
            LINES TERMINATED BY '%s'
            IGNORE 1 LINES;""" % (escaped_fn,
                                  os.linesep.encode('unicode_escape').decode()))
    assert result == expected


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
