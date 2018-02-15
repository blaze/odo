from __future__ import absolute_import, division, print_function

import os
from itertools import product
import pandas as pd
import pytest
sa = pytest.importorskip('sqlalchemy')

from datashape import dshape, discover
from odo import resource, odo
from odo.utils import tmpfile, filetext


ds = dshape('var *  {a: int32, b: int32}')
data = [(1, 2), (10, 20), (100, 200)]


@pytest.yield_fixture
def csv():
    with tmpfile('csv') as filename:
        yield odo(data, filename, dshape=ds, has_header=False)


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


def test_different_encoding():
    encoding = 'latin1'
    with tmpfile('db') as db:
        sql = odo(os.path.join(os.path.dirname(__file__), 'encoding.csv'),
                  'sqlite:///%s::t' % db, encoding=encoding)
        result = odo(sql, list)

    NULL = u''

    expected = [(u'1958.001.500131-1A', 1, NULL, NULL, 899),
                (u'1958.001.500156-6', 1, NULL, NULL, 899),
                (u'1958.001.500162-1', 1, NULL, NULL, 899),
                (u'1958.001.500204-2', 1, NULL, NULL, 899),
                (u'1958.001.500204-2A', 1, NULL, NULL, 899),
                (u'1958.001.500204-2B', 1, NULL, NULL, 899),
                (u'1958.001.500223-6', 1, NULL, NULL, 9610),
                (u'1958.001.500233-9', 1, NULL, NULL, 4703),
                (u'1909.017.000018-3', 1, 30.0, u'sumaria', 899)]
    assert result == expected


@pytest.yield_fixture
def quoted_sql():
    with tmpfile('.db') as db:
        try:
            yield resource('sqlite:///%s::foo bar' % db, dshape=ds)
        except sa.exc.OperationalError as e:
            pytest.skip(str(e))


@pytest.mark.xfail(
    raises=sa.exc.DatabaseError,
    reason='How do you use a quoted table name with the SQLite .import command?'
)
def test_quoted_name(csv, quoted_sql):
    with tmpfile('csv') as filename:
        csv = odo(data, filename, dshape=ds, has_header=True)
        s = odo(csv, quoted_sql)
        t = odo(csv, list)
        assert sorted(odo(s, list)) == sorted(t)


def test_different_encoding_to_csv():
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = odo(
                csvfilename,
                'sqlite:///%s::mytable' % dbfilename,
                encoding='latin1'
            )
            with tmpfile('.csv') as fn:
                with pytest.raises(ValueError):
                    odo(t, fn, encoding='latin1')


def test_send_parameterized_query_to_csv():
    with tmpfile('db') as dbfilename:
        with filetext('a,b\n1,2\n3,4', extension='csv') as csvfilename:
            t = odo(
                csvfilename,
                'sqlite:///%s::mytable' % dbfilename,
            )
        with tmpfile('.csv') as fn:
            q = t.select(t.c.a == 1)
            r = odo(q, fn)
            assert sorted(odo(q, list)) == sorted(odo(r, list))


def test_df_to_in_memory_db():
    df = pd.DataFrame([[1, 2], [3, 4]], columns=list('ab'))
    tbl = odo(df, 'sqlite:///:memory:::tbl')
    pd.util.testing.assert_frame_equal(
        odo(tbl, pd.DataFrame),
        df,
    )
