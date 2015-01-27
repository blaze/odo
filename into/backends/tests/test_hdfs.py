from into.backends.hdfs import discover, HDFS, CSV, create_command
from into.backends.sql import resource
import sqlalchemy as sa
from pywebhdfs.webhdfs import PyWebHdfsClient
from datashape import dshape
from collections import namedtuple


hdfs = PyWebHdfsClient(host='54.91.57.226', port='14000', user_name='hdfs')
data = HDFS(CSV)('/user/hive/warehouse/csv_test/data.csv', hdfs=hdfs)
ProxyTable = namedtuple('ProxyTable', 'table_name,db_name')


def test_discover():
    assert discover(data) == \
            dshape('var * {Name: string, RegistrationDate: datetime, ZipCode: int64, Consts: float64}')


def normalize(s):
    return ' '.join(s.split())

def test_create_hive():

    text = create_command('hive', ProxyTable('mytable', 'mydb'), data)
    expected = r"""
        CREATE TABLE mydb.mytable (
                          Name  STRING,
              RegistrationDate  TIMESTAMP,
                       ZipCode  SMALLINT,
                        Consts  DOUBLE
            )
        ROW FORMAT FIELDS DELIMITED BY ,
                   ESCAPED BY \
        STORED AS TEXTFILE
        LOCATION '/user/hive/warehouse/csv_test/data.csv'

        TBLPROPERTIES ("skip.header.line.count"="1");
        """

    assert normalize(text) == normalize(expected)
