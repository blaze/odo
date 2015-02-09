from __future__ import print_function

import pytest
import sys

pytestmark = pytest.mark.skipif(sys.platform == 'win32',
                                reason='Requires Mac or Linux')

sa = pytest.importorskip('sqlalchemy')
boto = pytest.importorskip('boto')
pytest.importorskip('psycopg2')
pytest.importorskip('redshift_sqlalchemy')

import os
import itertools
from contextlib import contextmanager

from into import into, resource, S3, discover, CSV, drop, append
from into.backends.aws import get_s3_connection
from into.utils import tmpfile

import pandas as pd
import pandas.util.testing as tm

import datashape
from datashape import string, float64, int64

from boto.exception import S3ResponseError


tips_uri = 's3://nyqpug/tips.csv'

df = pd.DataFrame({
    'a': list('abc'),
    'b': [1, 2, 3],
    'c': [1.0, 2.0, 3.0]
})[['a', 'b', 'c']]


js = pd.io.json.loads(pd.io.json.dumps(df, orient='records'))


@pytest.fixture
def db():
    key = os.environ.get('REDSHIFT_DB_URI', None)
    if not key:
        pytest.skip('Please define a non-empty environment variable called '
                    'REDSHIFT_DB_URI to test redshift <- S3')
    else:
        return key


@pytest.yield_fixture
def temp_tb(db):
    t = '%s::%s' % (db, next(_tmps))
    try:
        yield t
    finally:
        drop(resource(t))


@pytest.yield_fixture
def tmpcsv():
    with tmpfile('.csv') as fn:
        with open(fn, mode='w') as f:
            df.to_csv(f, index=False)
        yield fn


@contextmanager
def s3_bucket(extension):
    with conn():
        b = 's3://%s/%s%s' % (test_bucket_name, next(_tmps), extension)
        try:
            yield b
        finally:
            drop(resource(b))


@contextmanager
def conn():
    # requires that you have a config file or envars defined for credentials
    # this code makes me hate exceptions
    try:
        conn = get_s3_connection()
    except S3ResponseError:
        pytest.skip('unable to connect to s3')
    else:
        try:
            grants = conn.get_bucket(test_bucket_name).get_acl().acl.grants
        except S3ResponseError:
            pytest.skip('no permission to read on bucket %s' %
                        test_bucket_name)
        else:
            if not any(g.permission == 'FULL_CONTROL' or
                       g.permission == 'READ' for g in grants):
                pytest.skip('no permission to read on bucket %s' %
                            test_bucket_name)
            else:
                yield conn


test_bucket_name = 'into-redshift-csvs'


_tmps = ('tmp%d' % i for i in itertools.count())


def test_s3_resource():
    csv = resource(tips_uri)
    assert isinstance(csv, S3(CSV))


def test_s3_discover():
    csv = resource(tips_uri)
    assert isinstance(discover(csv), datashape.DataShape)


def test_s3_to_local_csv():
    with tmpfile('.csv') as fn:
        csv = into(fn, tips_uri)
        path = os.path.abspath(csv.path)
        assert os.path.exists(path)


def test_csv_to_s3_append():
    df = tm.makeMixedDataFrame()
    with tmpfile('.csv') as fn:
        with s3_bucket('.csv') as b:
            s3 = resource(b)
            df.to_csv(fn, index=False)
            append(s3, CSV(fn))
            result = into(pd.DataFrame, s3)
    tm.assert_frame_equal(df, result)


def test_csv_to_s3_into():
    df = tm.makeMixedDataFrame()
    with tmpfile('.csv') as fn:
        with s3_bucket('.csv') as b:
            df.to_csv(fn, index=False)
            s3 = into(b, CSV(fn))
            result = into(pd.DataFrame, s3)
    tm.assert_frame_equal(df, result)


def test_s3_to_redshift(temp_tb):
    s3 = resource(tips_uri)
    table = into(temp_tb, s3)

    assert discover(table) == discover(s3)
    assert into(set, table) == into(set, s3)


@pytest.mark.timeout(10)
def test_redshift_getting_started(temp_tb):
    dshape = datashape.dshape("""var * {
        userid: int64,
        username: ?string[8],
        firstname: ?string[30],
        lastname: ?string[30],
        city: ?string[30],
        state: ?string[2],
        email: ?string[100],
        phone: ?string[14],
        likesports: ?bool,
        liketheatre: ?bool,
        likeconcerts: ?bool,
        likejazz: ?bool,
        likeclassical: ?bool,
        likeopera: ?bool,
        likerock: ?bool,
        likevegas: ?bool,
        likebroadway: ?bool,
        likemusicals: ?bool,
    }""")
    csv = S3(CSV)('s3://awssampledb/tickit/allusers_pipe.txt')
    table = into(temp_tb, csv, dshape=dshape, delimiter='|')

    # make sure we have a non empty table
    assert table.count().execute().scalar() == 49989


def test_frame_to_s3_to_frame():
    with s3_bucket('.csv') as b:
        s3_csv = into(b, df)
        result = into(pd.DataFrame, s3_csv)
    tm.assert_frame_equal(result, df)


def test_csv_to_redshift(tmpcsv, temp_tb):
    assert into(set, into(temp_tb, tmpcsv)) == into(set, tmpcsv)


def test_frame_to_redshift(temp_tb):
    tb = into(temp_tb, df)
    assert into(set, tb) == into(set, df)


def test_textfile_to_s3():
    text = 'A cow jumped over the moon'
    with tmpfile('.txt') as fn:
        with s3_bucket('.txt') as b:
            with open(fn, mode='w') as f:
                f.write(os.linesep.join(text.split()))
            result = into(b, resource(fn))
    assert discover(result) == datashape.dshape('var * string')


def test_jsonlines_to_s3():
    with tmpfile('.json') as fn:
        with open(fn, mode='w') as f:
            for row in js:
                f.write(pd.io.json.dumps(row))
                f.write(os.linesep)
        with s3_bucket('.json') as b:
            result = into(b, resource(fn))
            assert discover(result) == discover(js)


def test_s3_jsonlines_discover():
    json_dshape = discover(resource('s3://nyqpug/tips.json'))
    names = list(map(str, sorted(json_dshape.measure.names)))
    assert names == ['day', 'sex', 'size', 'smoker', 'time', 'tip',
                     'total_bill']
    types = [json_dshape.measure[name] for name in names]
    assert types == [string, string, int64, string, string, float64, float64]


def test_s3_csv_discover():
    result = discover(resource('s3://nyqpug/tips.csv'))
    expected = datashape.dshape("""var * {
      total_bill: ?float64,
      tip: ?float64,
      sex: ?string,
      smoker: ?string,
      day: ?string,
      time: ?string,
      size: int64
      }""")
    assert result == expected


def test_s3_to_sqlite():
    with tmpfile('.db') as fn:
        tb = into('sqlite:///%s::tips' % fn, tips_uri,
                  dshape=discover(resource(tips_uri)))
        lhs = into(list, tb)
        assert lhs == into(list, tips_uri)
