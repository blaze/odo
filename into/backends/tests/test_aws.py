import pytest
from into import into, resource, S3, discover, CSV
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


@pytest.mark.xfail(raises=NotImplementedError,
                   reason=('need to figure out how to get a redshift instance '
                           'for testing'))
def test_s3_to_redshift():
    redshift_uri = ('redshift+psycopg2://username@host.amazonaws.com:5439/'
                    'database::t')
    table = into(redshift_uri, 's3://bucket/csvdir')
    assert isinstance(table, sa.Table)
    assert table.name == 't'
