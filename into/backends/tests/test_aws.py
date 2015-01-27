import pytest
import os
from into import into, resource, S3, discover, CSV, drop
from into.utils import tmpfile
import pandas as pd
import pandas.util.testing as tm
import datashape

sa = pytest.importorskip('sqlalchemy')
pytest.importorskip('psycopg2')
pytest.importorskip('redshift_sqlalchemy')


tips_uri = 's3://nyqpug/tips.csv'


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


@pytest.mark.xfail(raises=IOError, reason='not implemented yet')
def test_frame_to_s3():
    df = pd.DataFrame({
        'a': list('abc'),
        'b': [1, 2, 3],
        'c': [1.0, 2.0, 3.0]
    })[['a', 'b', 'c']]

    to_this = S3(CSV)('s3://nyqpug/test.csv')
    s3_csv = into(to_this, df)
    result = into(pd.DataFrame, s3_csv)
    tm.assert_frame_equal(result, df)


@pytest.mark.skipif('REDSHIFT_DB_URI' not in os.environ,
                    reason=('Please define an environment variable called '
                            'REDSHIFT_DB_URI to test redshift <- S3'))
def test_s3_to_redshift():
    redshift_uri = '%s::tips' % os.environ['REDSHIFT_DB_URI']
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
