import pytest
import os
import itertools
from into import into, resource, S3, discover, CSV, drop, append
from into.backends.aws import get_s3_connection
from into.utils import tmpfile
import pandas as pd
import pandas.util.testing as tm
import datashape

sa = pytest.importorskip('sqlalchemy')
boto = pytest.importorskip('boto')
pytest.importorskip('psycopg2')
pytest.importorskip('redshift_sqlalchemy')


tips_uri = 's3://nyqpug/tips.csv'


@pytest.fixture
def db():
    key = os.environ.get('REDSHIFT_DB_URI', None)
    if not key:
        pytest.skip('Please define a non-empty environment variable called '
                    'REDSHIFT_DB_URI to test redshift <- S3')
    else:
        return key


@pytest.fixture(scope='module')
def large_file():
    try:
        filename = os.environ['LARGE_REDSHIFT_CSV']
    except KeyError:
        pytest.skip("no envar LARGE_REDSHIFT_CSV")
    else:
        if os.path.getsize(filename) < 100 * 1 << 20:
            pytest.skip('file must be at least 100MB in size')
        else:
            return filename


@pytest.fixture
def temp_tb(db):
    return '%s::%s' % (db, next(_tmps))


@pytest.fixture(scope='module')
def conn():
    # requires that you have a config file or envars defined for credentials
    try:
        conn = get_s3_connection()
    except boto.exception.S3ResponseError:
        pytest.skip('unable to connect to s3')
    else:
        try:
            grants = conn.get_bucket(test_bucket_name).get_acl().acl.grants
        except boto.exception.S3ResponseError:
            pytest.skip('no permission to read on bucket %s' %
                        test_bucket_name)
        else:
            if not any(g.permission == 'FULL_CONTROL' or
                       g.permission == 'READ' for g in grants):
                pytest.skip('no permission to read on bucket %s' %
                            test_bucket_name)
            else:
                return conn


test_bucket_name = 'into-redshift-csvs'


_tmps = ('tmp%d' % i for i in itertools.count())


@pytest.yield_fixture
def s3_bucket(conn):
    key = next(_tmps)
    b = 's3://%s/%s.csv' % (test_bucket_name, key)
    yield b
    drop(resource(b))


@pytest.yield_fixture
def gz_s3_bucket(conn):
    key = next(_tmps)
    b = 's3://%s/%s.csv.gz' % (test_bucket_name, key)
    yield b
    drop(resource(b))


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


def test_csv_to_s3_append(s3_bucket):
    df = tm.makeMixedDataFrame()
    s3 = resource(s3_bucket)
    with tmpfile('.csv') as fn:
        df.to_csv(fn, index=False)
        append(s3, CSV(fn))
    result = into(pd.DataFrame, s3)
    tm.assert_frame_equal(df, result)


def test_csv_to_s3_into(s3_bucket):
    df = tm.makeMixedDataFrame()
    with tmpfile('.csv') as fn:
        df.to_csv(fn, index=False)
        s3 = into(s3_bucket, CSV(fn))
    result = into(pd.DataFrame, s3)
    tm.assert_frame_equal(df, result)


def test_s3_to_redshift(db):
    redshift_uri = '%s::tips' % db
    s3 = S3(CSV)('s3://nyqpug/tips.csv')
    table = into(redshift_uri, s3, dshape=discover(s3))
    dshape = discover(table)
    ds = datashape.dshape("""
    var * {
        total_bill: float64,
        tip: float64,
        sex: string,
        smoker: string,
        day: string,
        time: string,
        size: int64
    }""")
    try:
        # make sure our table is properly named
        assert table.name == 'tips'

        # make sure we have a non empty table
        assert table.bind.execute("select count(*) from tips;").scalar() == 244

        # make sure we have the proper column types
        assert dshape == ds
    finally:
        # don't hang around
        drop(table)


def test_redshift_getting_started(db):
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

    redshift_uri = '%s::users' % db
    csv = S3(CSV)('s3://awssampledb/tickit/allusers_pipe.txt')
    table = into(redshift_uri, csv, dshape=dshape, delimiter='|')
    n = 49989
    try:
        # make sure our table is properly named
        assert table.name == 'users'

        # make sure we have a non empty table
        assert table.bind.execute("select count(*) from users;").scalar() == n
    finally:
        # don't hang around
        drop(table)


@pytest.mark.xfail(raises=(IOError, AttributeError),
                   reason='No frame implementations yet')
def test_frame_to_s3_to_frame(s3_bucket):
    df = pd.DataFrame({
        'a': list('abc'),
        'b': [1, 2, 3],
        'c': [1.0, 2.0, 3.0]
    })[['a', 'b', 'c']]

    s3_csv = into(s3_bucket, df)
    result = into(pd.DataFrame, s3_csv)
    tm.assert_frame_equal(result, df)


@pytest.mark.xfail(raises=ValueError,
                   reason='gzip not implemented for multipart uploads')
def test_large_gzip_to_redshift(large_file, gz_s3_bucket, temp_tb):
    s3 = into(gz_s3_bucket, large_file)
    table = into(temp_tb, s3)
    drop(table)


def test_large_raw_to_redshift(large_file, s3_bucket, temp_tb):
    s3 = into(s3_bucket, large_file)
    table = into(temp_tb, s3)
    try:
        assert (table.bind.execute("select count(*) from %s;" %
                temp_tb.split('::', 1)[1]).scalar() == 1 << 20)
    finally:
        drop(table)
