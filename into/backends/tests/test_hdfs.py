from into.backends.hdfs import (discover, HDFS, CSV, TableProxy, SSH)
from into.backends.sql import resource
from into import into, drop
import sqlalchemy as sa
from pywebhdfs.webhdfs import PyWebHdfsClient
from datashape import dshape
from into.directory import Directory
import os

host = '' or os.environ.get('HDFS_TEST_HOST')

if not host:
    import pytest
    pytest.importorskip('does_not_exist')


hdfs = PyWebHdfsClient(host=host, port='14000', user_name='hdfs')
hdfs_csv= HDFS(CSV)('/user/hive/mrocklin/accounts/accounts.csv', hdfs=hdfs)
hdfs_directory = HDFS(Directory(CSV))('/user/hive/mrocklin/accounts/', hdfs=hdfs)
ds = dshape('var * {id: ?int64, name: ?string, amount: ?int64}')
engine = resource('hive://hdfs@%s:10000/default' % host)


def test_discover():
    assert discover(hdfs_csv) == \
            dshape('var * {id: int64, name: string, amount: int64}')

def test_discover_hdfs_directory():
    assert discover(hdfs_directory) == \
            dshape('var * {id: int64, name: string, amount: int64}')


def normalize(s):
    return ' '.join(s.split())


auth = {'hostname': host,
        'key_filename': os.path.expanduser('~/.ssh/cdh_testing.key'),
        'username': 'ubuntu'}

ssh_csv= SSH(CSV)('/home/ubuntu/accounts.csv', **auth)
ssh_directory = SSH(Directory(CSV))('/home/ubuntu/mrocklin/', **auth)


def test_hdfs_hive_creation():
    uri = 'hive://hdfs@%s:10000/default::hdfs_directory_1' % host
    try:
        t = into(uri, hdfs_directory)
        assert isinstance(t, sa.Table)
        assert len(into(list, t)) > 0
        assert discover(t) == ds
    finally:
        drop(t)


def test_ssh_hive_creation():
    uri = 'hive://hdfs@%s:10000/default::ssh_1' % host
    try:
        t = into(uri, ssh_csv)
        assert isinstance(t, sa.Table)
        assert len(into(list, t)) > 0
    finally:
        drop(t)



def test_ssh_directory_hive_creation():
    uri = 'hive://hdfs@%s:10000/default::ssh_2' % host
    try:
        t = into(uri, ssh_directory)
        assert isinstance(t, sa.Table)
        assert discover(t) == ds
        assert len(into(list, t)) > 0
        assert len(into(list, t)) == 8

    finally:
        drop(t)


def test_ssh_hive_creation_with_full_urls():
    uri = 'hive://hdfs@%s:10000/default::ssh_3' % host
    try:
        t = into(uri, 'ssh://ubuntu@%s:accounts.csv' % host,
                 key_filename='/home/mrocklin/.ssh/cdh_testing.key')
        assert isinstance(t, sa.Table)
        n = len(into(list, t))
        assert n > 0

        # Load it again
        into(t, 'ssh://ubuntu@%s:accounts.csv' % host,
             key_filename='/home/mrocklin/.ssh/cdh_testing.key')

        # Doubles length
        assert len(into(list, t)) == 2 * n
    finally:
        drop(t)
