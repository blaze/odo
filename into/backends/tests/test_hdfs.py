from into.backends.hdfs import (discover, HDFS, CSV,
        create_hive_from_hdfs_directory_of_csvs,
        create_hive_from_remote_csv_file, TableProxy, SSH)
from into.backends.sql import resource
from into import into
import sqlalchemy as sa
from pywebhdfs.webhdfs import PyWebHdfsClient
from datashape import dshape
from into.directory import Directory


hdfs = PyWebHdfsClient(host='54.91.57.226', port='14000', user_name='hdfs')
hdfs_csv= HDFS(CSV)('/user/hive/warehouse/csv_test/data.csv', hdfs=hdfs)
hdfs_directory = HDFS(Directory(CSV))('/user/hive/mrocklin/accounts/', hdfs=hdfs)
engine = resource('hive://hdfs@54.91.57.226:10000/default')


def test_discover():
    assert discover(hdfs_csv) == \
            dshape('var * {Name: string, RegistrationDate: datetime, ZipCode: int64, Consts: float64}')

def test_discover_hdfs_directory():
    assert discover(hdfs_directory) == \
            dshape('var * {id: int64, name: string, amount: int64}')


def normalize(s):
    return ' '.join(s.split())

def test_create_hive():
    text = create_hive_from_hdfs_directory_of_csvs(
            TableProxy(engine, 'mytable'), hdfs_directory)
    expected = r"""
        CREATE EXTERNAL TABLE default.mytable (
                      id  BIGINT,
                    name  STRING,
                  amount  BIGINT
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/user/hive/mrocklin/accounts/'

        TBLPROPERTIES ("skip.header.line.count"="1")
        """

    assert normalize(text) == normalize(expected)


auth = {'hostname': '54.91.57.226',
        'key_filename': '/home/mrocklin/.ssh/cdh_testing.key',
        'username': 'ubuntu'}

ssh_csv= SSH(CSV)('accounts.csv', **auth)
ssh_directory = SSH(Directory(CSV))('mrocklin/', **auth)


def test_create_hive_from_remote_csv_file():
    tbl = TableProxy(engine, 'mytable')
    ds = discover(ssh_directory)
    text = create_hive_from_remote_csv_file(tbl, ssh_directory, dshape=ds)

    expected = r"""
        CREATE TABLE default.mytable (
                      id  BIGINT,
                    name  STRING,
                  amount  BIGINT
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
        STORED AS TEXTFILE

        TBLPROPERTIES ("skip.header.line.count"="1")
        """

    assert normalize(text) == normalize(expected)

    text = create_hive_from_remote_csv_file(tbl, ssh_csv, dshape=ds)

    expected = r"""
        CREATE TABLE default.mytable (
                      id  BIGINT,
                    name  STRING,
                  amount  BIGINT
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
        STORED AS TEXTFILE

        TBLPROPERTIES ("skip.header.line.count"="1")
        """

    assert normalize(text) == normalize(expected)


def test_ssh_hive_creation():
    try:
        drop('hive://hdfs@54.91.57.226:10000/default::tmp_1')
    except:
        pass

    t = into('hive://hdfs@54.91.57.226:10000/default::tmp_1', ssh_csv)
    assert isinstance(t, sa.Table)
    assert len(into(list, t)) == 5


def test_ssh_directory_hive_creation():
    try:
        drop('hive://hdfs@54.91.57.226:10000/default::tmp_2')
    except:
        pass

    t = into('hive://hdfs@54.91.57.226:10000/default::tmp_2', ssh_directory)
    assert isinstance(t, sa.Table)
    assert discover(t) == discover(ssh_directory)
    assert len(into(list, t)) == 8


def test_ssh_hive_creation_with_full_urls():
    try:
        drop('hive://hdfs@54.91.57.226:10000/default::tmp_3')
    except:
        pass
    t = into('hive://hdfs@54.91.57.226:10000/default::tmp_3',
             'ssh://ubuntu@54.91.57.226:accounts.csv',
             key_filename='/home/mrocklin/.ssh/cdh_testing.key')
    assert isinstance(t, sa.Table)
    assert len(into(list, t)) == 5
