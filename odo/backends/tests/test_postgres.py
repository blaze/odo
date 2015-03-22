from __future__ import absolute_import, division, print_function

import pytest

sa = pytest.importorskip('sqlalchemy')
psycopg2 = pytest.importorskip('psycopg2')

import itertools

from odo.backends.csv import CSV
from odo import into, resource, drop, discover
from odo.utils import assert_allclose, tmpfile
import os
import getpass


names = ('tbl%d' % i for i in itertools.count())
data = [(1, 2), (10, 20), (100, 200)]


@pytest.yield_fixture(scope='module')
def csv():
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write('\n'.join(','.join(map(str, row)) for row in data))
        yield CSV(fn)


@pytest.fixture
def complex_csv():
    this_dir = os.path.dirname(__file__)
    return CSV(os.path.join(this_dir, 'dummydata.csv'), has_header=True)


@pytest.fixture
def url():
    return 'postgresql://postgres@localhost/test::%s' % next(names)


@pytest.yield_fixture
def sql(url):
    try:
        t = resource(url, dshape='var * {a: int32, b: int32}')
    except sa.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        yield t
        drop(t)


@pytest.yield_fixture
def complex_sql(url):
    ds = """var * {
        Name: string, RegistrationDate: date, ZipCode: int32, Consts: float64
    }"""
    try:
        t = resource(url, dshape=ds)
    except sa.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        yield t
        drop(t)


def test_simple_into(csv, sql):
    into(sql, csv, dshape=discover(sql))
    assert into(list, sql) == data


def test_append(csv, sql):
    into(sql, csv)
    assert into(list, sql) == data

    into(sql, csv)
    assert into(list, sql) == data + data


def test_tryexcept_into(csv, sql):
    into(sql, csv, quotechar="alpha")  # uses multi-byte character
    assert into(list, sql) == data


def test_failing_argument(csv, sql):
    # this will start to fail if we ever restrict kwargs
    into(sql, csv, skipinitialspace="alpha")  # failing call


def test_no_header_no_columns(csv, sql):
    into(sql, csv, dshape=discover(sql))
    assert into(list, sql) == data


def test_complex_into(complex_csv, complex_sql):
    # data from: http://dummydata.me/generate
    into(complex_sql, complex_csv, dshape=discover(complex_sql))
    assert_allclose(into(list, complex_sql), into(list, complex_csv))
