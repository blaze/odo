from into.backends.hdfs import discover, HDFS, CSV, create_command, TableProxy
from into.backends.sql import resource
import sqlalchemy as sa
from pywebhdfs.webhdfs import PyWebHdfsClient
from datashape import dshape
from into.directory import Directory

hdfs = PyWebHdfsClient(host='54.91.57.226', port='14000', user_name='hdfs')
data = HDFS(CSV)('/user/hive/warehouse/csv_test/data.csv', hdfs=hdfs)
directory = HDFS(Directory(CSV))('/user/hive/mrocklin/accounts/', hdfs=hdfs)
engine = resource('hive://hdfs@54.91.57.226:10000/default')


def test_discover():
    assert discover(data) == \
            dshape('var * {Name: string, RegistrationDate: datetime, ZipCode: int64, Consts: float64}')

def test_discover_hdfs_directory():
    assert discover(directory) == \
            dshape('var * {id: int64, name: string, amount: int64}')


def normalize(s):
    return ' '.join(s.split())

def test_create_hive():
    text = create_command('hive', TableProxy(engine, 'mytable'), directory)
    expected = r"""
        CREATE EXTERNAL TABLE default.mytable (
                      id  SMALLINT,
                    name  STRING,
                  amount  SMALLINT
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/user/hive/mrocklin/accounts/'

        TBLPROPERTIES ("skip.header.line.count"="1")
        """

    assert normalize(text) == normalize(expected)
