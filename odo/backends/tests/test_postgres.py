from __future__ import absolute_import, division, print_function

import pytest
import subprocess
import itertools
import re

sa = pytest.importorskip('sqlalchemy')
psycopg2 = pytest.importorskip('psycopg2')

ps = subprocess.Popen("ps aux | grep postgres",
                      shell=True, stdout=subprocess.PIPE)
output = ps.stdout.read()
num_processes = len(output.splitlines())
pytestmark = pytest.mark.skipif(num_processes < 6,
                                reason="No Postgres Installation")


from datashape import dshape
from odo.backends.csv import CSV
from odo.backends.sql import create_from_datashape
from odo import into, resource
from odo.utils import assert_allclose
import os
import csv as csv_module


url = 'postgresql://postgres:postgres@localhost'
file_name = 'test.csv'

_names = ('table%d' % i for i in itertools.count())


try:
    engine = sa.create_engine(url)
    name = 'tmpschema'
    create = sa.schema.CreateSchema(name)
    engine.execute(create)
    metadata = sa.MetaData()
    metadata.reflect(engine, schema=name)
    drop = sa.schema.DropSchema(name)
    engine.execute(drop)
except sa.exc.OperationalError:
    pytest.skip("Cannot connect to postgres")


@pytest.fixture
def tbl():
    return next(_names)


data = [(1, 2), (10, 20), (100, 200)]
ds = dshape('var * {a: int32, b: int32}')


def setup_function(function):
    with open(file_name, 'w') as f:
        csv_writer = csv_module.writer(f)
        for row in data:
            csv_writer.writerow(row)


def teardown_function(function):
    os.remove(file_name)
    engine = sa.create_engine(url)
    metadata = sa.MetaData()
    metadata.reflect(engine)

    for t in metadata.tables:
        if re.match(r'^table\d+$', t) is not None:
            metadata.tables[t].drop(engine)


def test_csv_postgres_load(tbl):
    engine = sa.create_engine(url)

    if engine.has_table(tbl):
        metadata = sa.MetaData()
        metadata.reflect(engine)
        t = metadata.tables[tbl]
        t.drop(engine)

    create_from_datashape(engine, dshape('{%s: %s}' % (tbl, ds)))
    metadata = sa.MetaData()
    metadata.reflect(engine)
    conn = engine.raw_connection()

    cursor = conn.cursor()
    full_path = os.path.abspath(file_name)
    load = '''copy {0} from '{1}'(FORMAT CSV, DELIMITER ',', NULL '');'''.format(tbl, full_path)
    cursor.execute(load)
    conn.commit()


def test_simple_into(tbl):
    csv = CSV(file_name)
    sql = resource(url, tbl, dshape=ds)

    into(sql, csv, dshape=ds)

    assert into(list, sql) == data


def test_append(tbl):
    csv = CSV(file_name)
    sql = resource(url, tbl, dshape=ds)

    into(sql, csv)
    assert into(list, sql) == data

    into(sql, csv)
    assert into(list, sql) == data + data


def test_tryexcept_into(tbl):
    csv = CSV(file_name)
    sql = resource(url, tbl, dshape=ds)

    into(sql, csv, quotechar="alpha")  # uses multi-byte character
    assert into(list, sql) == data


def test_failing_argument(tbl):
    # this will start to fail if we ever restrict kwargs
    csv = CSV(file_name)
    sql = resource(url, tbl, dshape=ds)

    into(sql, csv, skipinitialspace="alpha")  # failing call


def test_no_header_no_columns(tbl):
    csv = CSV(file_name)
    sql = resource(url, tbl, dshape=ds)

    into(sql, csv, dshape=ds)

    assert into(list, sql) == data


def test_complex_into(tbl):
    # data from: http://dummydata.me/generate
    this_dir = os.path.dirname(__file__)
    file_name = os.path.join(this_dir, 'dummydata.csv')

    ds = dshape("""
    var * {
        Name: string, RegistrationDate: date, ZipCode: int32, Consts: float64
    }""")

    csv = CSV(file_name, has_header=True)
    sql = resource(url, tbl, dshape=ds)

    into(sql, csv)

    assert_allclose(into(list, sql), into(list, csv))
