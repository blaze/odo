from __future__ import absolute_import, division, print_function

import pytest
import os

pywebhdfs = pytest.importorskip('pywebhdfs')
pyhive = pytest.importorskip('pyhive')

host = os.environ.get('HDFS_TEST_HOST')
pytestmark = pytest.mark.skipif(host is None,
                                reason='No HDFS_TEST_HOST envar defined')

from pywebhdfs.webhdfs import PyWebHdfsClient

import uuid
from into.backends.hdfs import discover, HDFS, CSV, SSH
from into.backends.sql import resource
from into import into, drop, JSONLines
from into.utils import filetext, ignoring, tmpfile
import sqlalchemy as sa
from datashape import dshape
from into.directory import Directory
from contextlib import contextmanager

hdfs = PyWebHdfsClient(host=host, port='14000', user_name='hdfs')
ds = dshape('var * {id: ?int64, name: ?string, amount: ?int64}')
engine = resource('hive://hdfs@%s:10000/default' % host)


accounts_1_csv = """
id,name,amount
1,Alice,100
2,Bob,200
3,Charlie,300
4,Dan,400
5,Edith,500""".strip()

accounts_2_csv = """
id,name,amount
6,Frank,600
7,George,700
8,Hannah,800
""".strip()


@contextmanager
def accounts_data():
    a = '/user/hive/test/accounts/accounts.1.csv'
    b = '/user/hive/test/accounts/accounts.2.csv'
    hdfs.make_dir('user/hive/test/accounts')
    hdfs.create_file(a.lstrip('/'), accounts_1_csv)
    hdfs.create_file(b.lstrip('/'), accounts_2_csv)

    A = HDFS(CSV)(a, hdfs=hdfs)
    B = HDFS(CSV)(b, hdfs=hdfs)
    directory = HDFS(Directory(CSV))('/user/hive/test/accounts/', hdfs=hdfs)

    try:
        yield (directory, (A, B))
    finally:
        hdfs.delete_file_dir(a)
        hdfs.delete_file_dir(b)



def test_discover():
    with accounts_data() as (directory, (a, b)):
        assert str(discover(a)).replace('?', '') == \
                'var * {id: int64, name: string, amount: int64}'

        assert str(discover(directory)).replace('?', '') == \
                'var * {id: int64, name: string, amount: int64}'


@contextmanager
def tmpfile_hdfs(ext=''):
    fn = str(uuid.uuid1())
    if ext:
        fn = fn + '.' + ext

    try:
        yield fn
    finally:
        hdfs.delete_file_dir(fn)


def test_copy_local_files_to_hdfs():
    with tmpfile_hdfs() as target:
        with filetext('name,amount\nAlice,100\nBob,200') as source:
            csv = CSV(source)
            scsv = HDFS(CSV)(target, hdfs=hdfs)
            into(scsv, csv, blocksize=10)  # 10 bytes per message

            assert discover(scsv) == discover(csv)


def test_copy_hdfs_files_locally():
    with tmpfile('csv') as target:
        with accounts_data() as (d, (a, b)):
            csv = into(target, a)
            with open(csv.path) as f:
                assert f.read().strip() == accounts_1_csv

def test_copy_hdfs_data_into_memory():
    with accounts_data() as (d, (a, b)):
        assert into(list, a)


def test_HDFS_constructor_allows_user_alternatives():
    r = HDFS(CSV)('foo.csv', username='alice', host='host')
    assert r.hdfs.user_name == 'alice'


def test_hdfs_resource():
    r = resource('hdfs://user@hostname:1234:/path/to/myfile.json')
    assert isinstance(r, HDFS(JSONLines))
    assert r.hdfs.user_name == 'user'
    assert r.hdfs.host == 'hostname'
    assert r.hdfs.port == '1234'
    assert r.path == '/path/to/myfile.json'

    assert isinstance(resource('hdfs://path/to/myfile.csv',
                                host='host', user='user', port=1234),
                      HDFS(CSV))
    assert isinstance(resource('hdfs://path/to/*.csv',
                                host='host', user='user', port=1234),
                      HDFS(Directory(CSV)))


def test_hdfs_hive_creation():
    with accounts_data() as (hdfs_directory, _):
        with hive_table(host) as uri:
            t = into(uri, hdfs_directory)
            assert isinstance(t, sa.Table)
            assert len(into(list, t)) > 0
            assert discover(t) == ds


def normalize(s):
    return ' '.join(s.split())


auth = {'hostname': host,
        'key_filename': os.path.expanduser('~/.ssh/cdh_testing.key'),
        'username': 'ubuntu'}

ssh_csv= SSH(CSV)('/home/ubuntu/into-testing/accounts1.csv', **auth)
ssh_directory = SSH(Directory(CSV))('/home/ubuntu/into-testing/', **auth)


@contextmanager
def hive_table(host):
    name = ('temp' + str(uuid.uuid1()).replace('-', ''))[:30]
    uri = 'hive://hdfs@%s:10000/default::%s' % (host, name)

    try:
        yield uri
    finally:
        with ignoring(Exception):
            drop(uri)


def test_ssh_hive_creation():
    with hive_table(host) as uri:
        t = into(uri, ssh_csv, raise_on_errors=True)
        assert isinstance(t, sa.Table)
        assert len(into(list, t)) > 0


def test_hive_creation_from_local_file():
    with filetext(accounts_1_csv, extension='csv') as fn:
        with hive_table(host) as uri:
            t = into(uri, fn, **auth)
            assert isinstance(t, sa.Table)
            assert into(set, t) == into(set, fn)


def test_ssh_directory_hive_creation():
    with hive_table(host) as uri:
        t = into(uri, ssh_directory)
        assert isinstance(t, sa.Table)
        assert discover(t) == ds
        assert len(into(list, t)) > 0


def test_ssh_hive_creation_with_full_urls():
    with hive_table(host) as uri:
        t = into(uri, 'ssh://ubuntu@%s:accounts.csv' % host,
                 key_filename=os.path.expanduser('~/.ssh/cdh_testing.key'))
        assert isinstance(t, sa.Table)
        n = len(into(list, t))
        assert n > 0

        # Load it again
        into(t, 'ssh://ubuntu@%s:accounts.csv' % host,
             key_filename=os.path.expanduser('~/.ssh/cdh_testing.key'))

        # Doubles length
        assert len(into(list, t)) == 2 * n


def test_hive_resource():
    db = resource('hive://hdfs@%s:10000/default' % host)
    assert isinstance(db, sa.engine.Engine)

    db = resource('hive://%s/' % host)
    assert isinstance(db, sa.engine.Engine)
    assert str(db.url) == 'hive://hdfs@%s:10000/default' % host
