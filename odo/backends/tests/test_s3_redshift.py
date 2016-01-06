import os
import sys
import pytest

pytestmark = pytest.mark.skipif(sys.platform == 'win32',
                                reason='Requires Mac or Linux')

sa = pytest.importorskip('sqlalchemy')
boto = pytest.importorskip('boto')
pytest.importorskip('psycopg2')
pytest.importorskip('redshift_sqlalchemy')

from contextlib import closing
import json
import itertools
import pandas as pd

import datashape
from odo import into, resource, S3, discover, CSV, drop, odo
from odo.utils import tmpfile
from odo.compatibility import urlopen

with closing(urlopen('http://httpbin.org/ip')) as url:
    public_ip = json.loads(url.read().decode())['origin']
cidrip = public_ip + '/32'

_tmps = ('tmp%d' % i for i in itertools.count())

df = pd.DataFrame({
    'a': list('abc'),
    'b': [1, 2, 3],
    'c': [1.0, 2.0, 3.0]
})[['a', 'b', 'c']]

is_authorized = tried = False


@pytest.fixture(scope='module')
def rs_auth():
    # if we aren't authorized and we've tried to authorize then skip, prevents
    # us from having to deal with timeouts

    # TODO: this will fail if we want to use a testing cluster with a different
    # security group than 'default'
    global is_authorized, tried

    if not is_authorized and not tried:
        if not tried:
            try:
                conn = boto.connect_redshift()
            except boto.exception.NoAuthHandlerFound as e:
                pytest.skip('authorization to access redshift cluster failed '
                            '%s' % e)
            try:
                conn.authorize_cluster_security_group_ingress('default',
                                                              cidrip=cidrip)
            except boto.redshift.exceptions.AuthorizationAlreadyExists:
                is_authorized = True
            except Exception as e:
                pytest.skip('authorization to access redshift cluster failed '
                            '%s' % e)
            else:
                is_authorized = True
            finally:
                tried = True
        else:
            pytest.skip('authorization to access redshift cluster failed')


@pytest.fixture
def db(rs_auth):
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


tips_uri = 's3://nyqpug/tips.csv'


def test_s3_to_redshift(temp_tb):
    s3 = resource(tips_uri)
    table = into(temp_tb, s3)

    assert discover(table) == discover(s3)
    assert into(set, table) == into(set, s3)


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
    table = into(temp_tb, csv, dshape=dshape)

    # make sure we have a non empty table
    assert table.count().scalar() == 49990


def test_redshift_dwdate(temp_tb):
    dshape = datashape.dshape("""var * {
          key: int64,
          date: string[19],
          day_of_week: string[10],
          month: string[10],
          year: int64,
          year_month_num: int64,
          year_month: string[8],
          day_num_in_week: int64,
          day_num_in_month: int64,
          day_num_in_year: int64,
          month_num_in_year: int64,
          week_num_in_year: int64,
          selling_season: string[13],
          last_day_in_week_fl: string[1],
          last_day_in_month_fl: string[1],
          holiday_fl: string[1],
          weekday_fl: string[1]
    }""")
    # we have to pass the separator here because the date column has a comma
    # TODO: see if we can provide a better error message by querying
    # stl_load_errors
    assert odo(S3(CSV)('s3://awssampledb/ssbgz/dwdate'),
               temp_tb,
               delimiter='|',
               compression='gzip',
               dshape=dshape).count().scalar() == 2556


def test_csv_to_redshift(tmpcsv, temp_tb):
    assert into(set, into(temp_tb, tmpcsv)) == into(set, tmpcsv)


def test_frame_to_redshift(temp_tb):
    tb = into(temp_tb, df)
    assert into(set, tb) == into(set, df)
