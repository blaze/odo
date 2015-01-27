from into.backends.hdfs import discover, HDFS, CSV, create_command
from into.backends.sql import resource
import sqlalchemy as sa
from pywebhdfs.webhdfs import PyWebHdfsClient
from datashape import dshape
from into.directory import Directory

hdfs = PyWebHdfsClient(host='54.91.57.226', port='14000', user_name='hdfs')
data = HDFS(CSV)('/user/hive/warehouse/csv_test/data.csv', hdfs=hdfs)
directory = HDFS(Directory(CSV))('/user/hive/mrocklin/accounts/', hdfs=hdfs)


def test_discover():
    assert discover(data) == \
            dshape('var * {Name: string, RegistrationDate: datetime, ZipCode: int64, Consts: float64}')

def test_discover_hdfs_directory():
    assert discover(directory) == \
            dshape('var * {id: int64, name: string, amount: int64}')


def normalize(s):
    return ' '.join(s.split())

def dont_test_create_hive():

    engine = sa.create_engine('sqlite:///:memory:')
    text = create_command('hive', TableProxy('mytable', engine), data)
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
