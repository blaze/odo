from __future__ import absolute_import, division, print_function

import pytest

pymysql = pytest.importorskip('pymysql')

from datashape import var, DataShape, Record, dshape
import itertools
from odo.backends.csv import CSV
from odo import resource, odo
import sqlalchemy
import sqlalchemy as sa
import os
import sys
import csv as csv_module
import getpass
from odo import drop, discover
from odo.utils import tmpfile


pytestmark = pytest.mark.skipif(sys.platform == 'win32',
                                reason='not well tested on win32 mysql')

username = getpass.getuser()
url = 'mysql+pymysql://{0}@localhost:3306/test'.format(username)


def create_csv(data, file_name):
    with open(file_name, 'w') as f:
        csv_writer = csv_module.writer(f)
        for row in data:
            csv_writer.writerow(row)


data = [(1, 2), (10, 20), (100, 200)]
data_floats = [(1.02, 2.02), (102.02, 202.02), (1002.02, 2002.02)]


@pytest.yield_fixture
def csv():
    with tmpfile('.csv') as fn:
        create_csv(data, fn)
        yield CSV(fn)


@pytest.yield_fixture
def fcsv():
    with tmpfile('.csv') as fn:
        create_csv(data_floats, fn)
        yield CSV(fn, columns=list('ab'))


names = ('tbl%d' % i for i in itertools.count())


@pytest.fixture
def name():
    return next(names)


@pytest.fixture(scope='module')
def engine():
    return sqlalchemy.create_engine(url)


@pytest.yield_fixture
def sql(engine, csv, name):
    dshape = var * Record(list(zip('ab', discover(csv).measure.types)))
    try:
        t = resource('%s::%s' % (url, name), dshape=dshape)
    except sqlalchemy.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            yield t
        finally:
            drop(t)


@pytest.yield_fixture
def fsql(engine, fcsv, name):
    dshape = var * Record(list(zip('ab', discover(fcsv).measure.types)))
    try:
        t = resource('%s::%s' % (url, name), dshape=dshape)
    except sqlalchemy.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            yield t
        finally:
            drop(t)


@pytest.yield_fixture
def quoted_sql(engine, fcsv):
    dshape = var * Record(list(zip('ab', discover(fcsv).measure.types)))
    try:
        t = resource('%s::foo bar' % url, dshape=dshape)
    except sqlalchemy.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            yield t
        finally:
            drop(t)


@pytest.fixture
def dcsv():
    return CSV(os.path.join(os.path.dirname(__file__), 'dummydata.csv'))


@pytest.yield_fixture
def dsql(engine, dcsv, name):
    try:
        t = resource('%s::%s' % (url, name), dshape=discover(dcsv))
    except sqlalchemy.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            yield t
        finally:
            drop(t)


@pytest.yield_fixture
def decimal_sql(engine, name):
    try:
        t = resource('%s::%s' % (url, name),
                     dshape="var * {a: ?decimal[10, 3], b: decimal[11, 2]}")
    except sa.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            yield t
        finally:
            drop(t)



def test_csv_mysql_load(sql, csv):
    engine = sql.bind
    conn = engine.raw_connection()

    cursor = conn.cursor()
    full_path = os.path.abspath(csv.path)
    load = '''LOAD DATA INFILE '{0}' INTO TABLE {1} FIELDS TERMINATED BY ','
        lines terminated by '\n'
        '''.format(full_path, sql.name)
    cursor.execute(load)
    conn.commit()


def test_simple_into(sql, csv):
    odo(csv, sql, if_exists="replace")
    assert odo(sql, list) == [(1, 2), (10, 20), (100, 200)]


def test_append(sql, csv):
    odo(csv, sql, if_exists="replace")
    assert odo(sql, list) == [(1, 2), (10, 20), (100, 200)]

    odo(csv, sql, if_exists="append")
    assert odo(sql, list) == [(1, 2), (10, 20), (100, 200),
                              (1, 2), (10, 20), (100, 200)]


def test_simple_float_into(fsql, fcsv):
    odo(fcsv, fsql, if_exists="replace")

    assert odo(fsql, list) == [(1.02, 2.02),
                               (102.02, 202.02),
                               (1002.02, 2002.02)]


def test_tryexcept_into(sql, csv):
    # uses multi-byte character and fails over to using sql.extend()
    odo(csv, sql, if_exists="replace", QUOTE="alpha", FORMAT="csv")
    assert odo(sql, list) == [(1, 2), (10, 20), (100, 200)]


@pytest.mark.xfail(raises=KeyError)
def test_failing_argument(sql, csv):
    odo(csv, sql, if_exists="replace", skipinitialspace="alpha")


def test_no_header_no_columns(sql, csv):
    odo(csv, sql, if_exists="replace")
    assert odo(sql, list) == [(1, 2), (10, 20), (100, 200)]


@pytest.mark.xfail
def test_complex_into(dsql, dcsv):
    # data from: http://dummydata.me/generate
    odo(dcsv, dsql, if_exists="replace")
    assert odo(dsql, list) == odo(dcsv, list)


def test_sql_to_csv(sql, csv):
    sql = odo(csv, sql)
    with tmpfile('.csv') as fn:
        csv = odo(sql, fn)
        assert odo(csv, list) == data

        # explicitly test that we do NOT preserve the header here
        assert discover(csv).measure.names != discover(sql).measure.names


def test_sql_select_to_csv(sql, csv):
    sql = odo(csv, sql)
    query = sa.select([sql.c.a])
    with tmpfile('.csv') as fn:
        csv = odo(query, fn)
        assert odo(csv, list) == [(x,) for x, _ in data]


def test_csv_output_does_not_preserve_header(sql, csv):
    sql = odo(csv, sql)
    expected = "1,2\n10,20\n100,200\n"
    with tmpfile('.csv') as fn:
        csv = odo(sql, fn)
        with open(csv.path, 'rt') as f:
            result = f.read()
    assert result == expected


@pytest.mark.xfail(raises=AssertionError,
                   reason="Remove when all databases are being tested at once")
def test_different_encoding(name):
    encoding = 'latin1'
    try:
        sql = odo(os.path.join(os.path.dirname(__file__), 'encoding.csv'),
                  url + '::%s' % name,
                  encoding=encoding)
    except sa.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            result = odo(sql, list)
            expected = [(u'1958.001.500131-1A', 1, None, u'', 899),
                        (u'1958.001.500156-6', 1, None, u'', 899),
                        (u'1958.001.500162-1', 1, None, u'', 899),
                        (u'1958.001.500204-2', 1, None, u'', 899),
                        (u'1958.001.500204-2A', 1, None, u'', 899),
                        (u'1958.001.500204-2B', 1, None, u'', 899),
                        (u'1958.001.500223-6', 1, None, u'', 9610),
                        (u'1958.001.500233-9', 1, None, u'', 4703),
                        (u'1909.017.000018-3', 1, 30.0, u'sumaria', 899)]
            assert result == expected
        finally:
            drop(sql)


def test_decimal(decimal_sql):
    t = sa.Table(decimal_sql.name, sa.MetaData(decimal_sql.bind), autoload=True)
    assert discover(t) == dshape(
        "var * {a: ?decimal[10, 3], b: decimal[11, 2]}"
    )
    assert isinstance(t.c.a.type, sa.Numeric)
    assert isinstance(t.c.b.type, sa.Numeric)


def test_quoted_name(quoted_sql, fcsv):
    s = odo(fcsv, quoted_sql)
    t = odo(fcsv, list)
    assert sorted(odo(s, list)) == sorted(t)
