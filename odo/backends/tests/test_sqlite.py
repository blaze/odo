from __future__ import absolute_import, division, print_function

import os
from itertools import product
import pytest
pytest.importorskip('sqlalchemy')

from datashape import dshape, discover
from odo import resource, odo
from odo.utils import tmpfile, filetext


ds = dshape('var *  {a: int32, b: int32}')
data = [(1, 2), (10, 20), (100, 200)]


@pytest.yield_fixture
def csv():
    with tmpfile('csv') as filename:
        csv = odo(data, filename, dshape=ds, has_header=False)
        yield csv


def test_simple_into(csv):
    tbl = 'testtable'
    with tmpfile('db') as filename:
        engine = resource('sqlite:///' + filename)
        t = resource('sqlite:///' + filename + '::' + tbl,
                     dshape=ds)

        odo(csv, t, dshape=ds)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""SELECT
                        name
                       FROM
                        sqlite_master
                       WHERE type='table' and name='{0}';""".format(tbl))

        sqlite_tbl_names = cursor.fetchall()
        conn.close()

        assert sqlite_tbl_names[0][0] == tbl
        assert odo(t, list) == data


def test_csv_with_header():
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = odo(csvfilename, 'sqlite:///%s::mytable' % dbfilename)
            assert discover(t) == dshape('var * {a: int64, b: int64}')
            assert odo(t, set) == set([(1, 2), (3, 4)])


def test_csv_infer_header():
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = odo(csvfilename, 'sqlite:///%s::mytable' % dbfilename)
            assert discover(t) == dshape('var * {a: int64, b: int64}')
            assert odo(t, set) == set([(1, 2), (3, 4)])


@pytest.mark.parametrize(['sep', 'header'],
                         product([',', '|', '\t'], [True, False]))
def test_sqlite_to_csv(sep, header):
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = odo(csvfilename, 'sqlite:///%s::mytable' % dbfilename)

        with tmpfile('.csv') as fn:
            odo(t, fn, header=header, delimiter=sep)
            with open(fn, 'rt') as f:
                lines = f.readlines()
            expected = [tuple(map(int, row))
                        for row in map(lambda x: x.split(sep), lines[header:])]
            assert odo(fn, list, delimiter=sep, has_header=header,
                       dshape=discover(t)) == expected
