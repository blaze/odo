from __future__ import print_function

import pytest
import sys

pytestmark = pytest.mark.skipif(sys.platform == 'win32',
                                reason='Requires Mac or Linux')

boto = pytest.importorskip('boto')

import os
import itertools
import json
from contextlib import contextmanager, closing

import datashape
from datashape import string, float64, int64
from datashape.util.testing import assert_dshape_equal
import pandas as pd
import pandas.util.testing as tm

from odo import into, resource, S3, discover, CSV, drop, append, odo
from odo.backends.aws import get_s3_connection
from odo.utils import tmpfile
from odo.compatibility import urlopen


from boto.exception import S3ResponseError

tips_uri = 's3://nyqpug/tips.csv'

df = pd.DataFrame({
    'a': list('abc'),
    'b': [1, 2, 3],
    'c': [1.0, 2.0, 3.0]
})[['a', 'b', 'c']]


js = pd.io.json.loads(pd.io.json.dumps(df, orient='records'))

is_authorized = False
tried = False

with closing(urlopen('http://httpbin.org/ip')) as url:
    public_ip = json.loads(url.read().decode())['origin']

cidrip = public_ip + '/32'


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


@pytest.fixture
def s3_encryption_bucket():
    test_bucket = os.getenv('ODO_S3_ENCRYPTION_BUCKET')
    if not test_bucket:
        pytest.skip('No bucket defined that requires server-side encryption')
    return test_bucket


def test_s3_encrypted_upload(s3_encryption_bucket):
    s3_connection = boto.connect_s3()

    df = tm.makeMixedDataFrame()
    with tmpfile('.csv') as fn:
        df.to_csv(fn, index=False)
        s3_uri = 's3://{bucket}/{fn}'.format(bucket=s3_encryption_bucket, fn=os.path.basename(fn))
        odo(fn, s3_uri, s3=s3_connection, encrypt_key=True)
        result = odo(s3_uri, pd.DataFrame, s3=s3_connection)

    tm.assert_frame_equal(df, result)


def test_s3_encrypted_multipart_upload(s3_encryption_bucket):
    s3_connection = boto.connect_s3()

    df = tm.makeMixedDataFrame()
    with tmpfile('.csv') as fn:
        df.to_csv(fn, index=False)
        s3_uri = 's3://{bucket}/{fn}'.format(bucket=s3_encryption_bucket, fn=os.path.basename(fn))
        odo(fn, s3_uri, s3=s3_connection, encrypt_key=True, multipart=True)
        result = odo(s3_uri, pd.DataFrame, s3=s3_connection)

    tm.assert_frame_equal(df, result)


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


def test_frame_to_s3_to_frame():
    with s3_bucket('.csv') as b:
        s3_csv = into(b, df)
        result = into(pd.DataFrame, s3_csv)
    tm.assert_frame_equal(result, df)


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
      total_bill: float64,
      tip: float64,
      sex: ?string,
      smoker: ?string,
      day: ?string,
      time: ?string,
      size: int64
      }""")
    assert_dshape_equal(result, expected)


def test_s3_gz_csv_discover():
    result = discover(S3(CSV)('s3://nyqpug/tips.gz'))
    expected = datashape.dshape("""var * {
      total_bill: float64,
      tip: float64,
      sex: ?string,
      smoker: ?string,
      day: ?string,
      time: ?string,
      size: int64
      }""")
    assert_dshape_equal(result, expected)


def test_s3_to_sqlite():
    with tmpfile('.db') as fn:
        tb = into('sqlite:///%s::tips' % fn, tips_uri,
                  dshape=discover(resource(tips_uri)))
        lhs = into(list, tb)
        assert lhs == into(list, tips_uri)


def test_csv_to_s3__using_multipart_upload():
    df = pd.DataFrame({'a': ["*" * 5 * 1024 ** 2]})
    with tmpfile('.csv') as fn:
        with s3_bucket('.csv') as b:
            df.to_csv(fn, index=False)
            s3 = into(b, CSV(fn), multipart=True)
            result = into(pd.DataFrame, s3)
    tm.assert_frame_equal(df, result)


@pytest.mark.parametrize(
    ['prefix', 'suffix'],
    [
        pytest.mark.xfail(('xa', ''), raises=NotImplementedError),
        ('za', '.csv')
    ]
)
def test_chunks_of_s3(prefix, suffix):
    uri = 's3://nyqpug/{}*{}'.format(prefix, suffix)
    result = resource(uri)
    assert len(result.data) == 2
    expected = odo(tips_uri, pd.DataFrame)
    tm.assert_frame_equal(odo(result, pd.DataFrame), expected)
