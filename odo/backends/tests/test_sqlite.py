from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('sqlalchemy')

from datashape import dshape, discover
from odo.backends.csv import CSV
from odo import into, resource
from odo.utils import tmpfile, filetext
import sqlalchemy


ds = dshape('var *  {a: int32, b: int32}' )
data = [(1, 2), (10, 20), (100, 200)]

@pytest.yield_fixture
def csv():
    with tmpfile('csv') as filename:
        csv = into(filename, data, dshape=ds, has_header=False)
        yield csv


def test_simple_into(csv):
    tbl = 'testtable'
    with tmpfile('db') as filename:
        engine = sqlalchemy.create_engine('sqlite:///' + filename)
        t = resource('sqlite:///' + filename + '::' + tbl,
                     dshape=ds)

        into(t, csv, dshape=ds)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' and name='{0}';".format(tbl))

        sqlite_tbl_names = cursor.fetchall()
        conn.close()

        assert sqlite_tbl_names[0][0] == tbl
        assert into(list, t) == data


def test_csv_with_header():
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = into('sqlite:///%s::mytable' % dbfilename,
                     csvfilename, has_header=True)
            assert discover(t) == dshape('var * {a: int64, b: int64}')
            assert into(set, t) == set([(1, 2), (3, 4)])


def test_csv_infer_header():
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = into('sqlite:///%s::mytable' % dbfilename, csvfilename)
            assert discover(t) == dshape('var * {a: int64, b: int64}')
            assert into(set, t) == set([(1, 2), (3, 4)])
