from into.backends.hdfs import (discover, HDFS, CSV, TableProxy, SSH)
from into.backends.sql import resource
from into import into, drop
import sqlalchemy as sa
from pywebhdfs.webhdfs import PyWebHdfsClient
from datashape import dshape
from into.directory import Directory


hdfs = PyWebHdfsClient(host='54.91.57.226', port='14000', user_name='hdfs')
hdfs_csv= HDFS(CSV)('/user/hive/warehouse/csv_test/data.csv', hdfs=hdfs)
hdfs_directory = HDFS(Directory(CSV))('/user/hive/mrocklin/accounts/', hdfs=hdfs)
ds = dshape('var * {id: ?int64, name: ?string, amount: ?int64}')
engine = resource('hive://hdfs@54.91.57.226:10000/default')


def test_discover():
    assert discover(hdfs_csv) == \
            dshape('var * {Name: string, RegistrationDate: datetime, ZipCode: int64, Consts: float64}')

def test_discover_hdfs_directory():
    assert discover(hdfs_directory) == \
            dshape('var * {id: int64, name: string, amount: int64}')


def normalize(s):
    return ' '.join(s.split())


auth = {'hostname': '54.91.57.226',
        'key_filename': '/home/mrocklin/.ssh/cdh_testing.key',
        'username': 'ubuntu'}

ssh_csv= SSH(CSV)('/home/ubuntu/accounts.csv', **auth)
ssh_directory = SSH(Directory(CSV))('/home/ubuntu/mrocklin/', **auth)


def test_hdfs_hive_creation():
    uri = 'hive://hdfs@54.91.57.226:10000/default::hdfs_directory_1'
    try:
        t = into(uri, hdfs_directory)
        assert isinstance(t, sa.Table)
        assert len(into(list, t)) > 0
        assert discover(t) == ds
    finally:
        drop(t)


def test_ssh_hive_creation():
    uri = 'hive://hdfs@54.91.57.226:10000/default::ssh_1'
    try:
        t = into(uri, ssh_csv)
        assert isinstance(t, sa.Table)
        assert len(into(list, t)) > 0
    finally:
        drop(t)



def test_ssh_directory_hive_creation():
    uri = 'hive://hdfs@54.91.57.226:10000/default::ssh_2'
    try:
        t = into(uri, ssh_directory)
        assert isinstance(t, sa.Table)
        assert discover(t) == ds
        assert len(into(list, t)) > 0
        assert len(into(list, t)) == 8

    finally:
        drop(t)


def test_ssh_hive_creation_with_full_urls():
    uri = 'hive://hdfs@54.91.57.226:10000/default::ssh_3'
    try:
        t = into(uri, 'ssh://ubuntu@54.91.57.226:/home/ubuntu/accounts.csv',
                 key_filename='/home/mrocklin/.ssh/cdh_testing.key')
        assert isinstance(t, sa.Table)
        assert len(into(list, t)) > 0
        assert len(into(list, t)) == 5
    finally:
        drop(t)
