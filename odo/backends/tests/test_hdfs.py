from __future__ import absolute_import, division, print_function

import pytest
import os

pywebhdfs = pytest.importorskip('pywebhdfs')
pyhive = pytest.importorskip('pyhive')

host = os.environ.get('HDFS_TEST_HOST')
pytestmark = pytest.mark.skipif(host is None,
                                reason='No HDFS_TEST_HOST envar defined')

from pywebhdfs.webhdfs import PyWebHdfsClient

import pandas as pd
import numpy as np
import uuid
from odo.backends.hdfs import discover, HDFS, CSV, SSH, dialect_of, TableProxy
from odo.backends.sql import resource
from odo.backends.ssh import sftp
from odo import into, drop, JSONLines, odo
from odo.utils import filetext, ignoring, tmpfile
import sqlalchemy as sa
from datashape import dshape
from odo.directory import Directory
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

accounts_3_csv = """
id,name,amount
9,Isaac,900
10,Jane,1000
""".strip()

@contextmanager
def accounts_data():
    a = '/user/hive/test/accounts/accounts.1.csv'
    b = '/user/hive/test/accounts/accounts.2.csv'
    c = '/user/hive/test/accounts.3.csv'
    hdfs.make_dir('user/hive/test/accounts')
    hdfs.create_file(a.lstrip('/'), accounts_1_csv)
    hdfs.create_file(b.lstrip('/'), accounts_2_csv)
    hdfs.create_file(c.lstrip('/'), accounts_3_csv)

    A = HDFS(CSV)(a, hdfs=hdfs)
    B = HDFS(CSV)(b, hdfs=hdfs)
    C = HDFS(CSV)(c, hdfs=hdfs)
    directory = HDFS(Directory(CSV))('/user/hive/test/accounts/', hdfs=hdfs)

    try:
        yield (directory, (A, B, C))
    finally:
        hdfs.delete_file_dir(a)
        hdfs.delete_file_dir(b)
        hdfs.delete_file_dir(c)

@contextmanager
def accounts_ssh():
    """ Three csv files on the remote host in a directory """
    dirname = str(uuid.uuid1())
    conn = sftp(**auth)
    conn.mkdir(dirname)
    with filetext(accounts_1_csv) as fn:
        conn.put(fn, dirname + '/accounts.1.csv')
    with filetext(accounts_2_csv) as fn:
        conn.put(fn, dirname + '/accounts.2.csv')
    with filetext(accounts_3_csv) as fn:
        conn.put(fn, dirname + '/accounts.3.csv')

    filenames = [dirname + '/accounts.%d.csv' % i for i in [1, 2, 3]]
    uris = ['ssh://ubuntu@%s:%s' % (host, fn) for fn in filenames]

    try:
        yield 'ssh://ubuntu@%s:%s/*.csv' % (host, dirname),  uris
    finally:
        for fn in filenames:
            conn.remove(fn)
        conn.rmdir(dirname)


def test_discover():
    with accounts_data() as (directory, (a, b, c)):
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
        with accounts_data() as (d, (a, b, c)):
            csv = into(target, a)
            with open(csv.path) as f:
                assert f.read().strip() == accounts_1_csv


def test_copy_hdfs_data_into_memory():
    with accounts_data() as (d, (a, b, c)):
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


def normalize(s):
    return ' '.join(s.split())


auth = {'hostname': host,
        'key_filename': os.path.expanduser('~/.ssh/cdh_testing.key'),
        'username': 'ubuntu'}


@contextmanager
def hive_table(host):
    name = ('temp' + str(uuid.uuid1()).replace('-', ''))[:30]
    uri = 'hive://hdfs@%s:10000/default::%s' % (host, name)

    try:
        yield uri
    finally:
        with ignoring(Exception):
            drop(uri)


def test_hdfs_directory_hive_creation():
    with accounts_data() as (hdfs_directory, (a, b, c)):
        with hive_table(host) as uri:
            t = into(uri, hdfs_directory)
            assert isinstance(t, sa.Table)
            result = into(set, t)
            assert result > 0
            assert discover(t) == ds

            t2 = into(uri, c)  # append new singleton file
            assert len(into(list, t2)) > len(result)


def test_ssh_hive_creation():
    with hive_table(host) as uri:
        with accounts_ssh() as (_, (remote, _, _)):
            t = into(uri, remote, raise_on_errors=True, **auth)
            assert isinstance(t, sa.Table)
            assert into(set, t) == into(set, remote, **auth)

            # Load again
            t2 = into(uri, remote, raise_on_errors=True, **auth)
            assert isinstance(t2, sa.Table)
            assert len(into(list, t2)) == 2 * len(into(list, remote, **auth))


def test_hive_creation_from_local_file():
    with filetext(accounts_1_csv, extension='csv') as fn:
        with hive_table(host) as uri:
            t = into(uri, fn, **auth)
            assert isinstance(t, sa.Table)
            assert into(set, t) == into(set, fn)

            t2 = into(uri, fn, **auth)
            assert isinstance(t2, sa.Table)
            assert len(into(list, t2)) == 2 * len(into(list, fn))


def test_ssh_directory_hive_creation():
    with hive_table(host) as uri:
        with accounts_ssh() as (directory, _):
            t = odo(directory, uri, **auth)
            assert isinstance(t, sa.Table)
            assert discover(t) == ds
            assert len(into(list, t)) > 0


def test_ssh_hive_creation_with_full_urls():
    with hive_table(host) as uri:
        with accounts_ssh() as (_, (remote, _, _)):
            t = into(uri, remote,
                     key_filename=os.path.expanduser('~/.ssh/cdh_testing.key'))
            assert isinstance(t, sa.Table)
            n = len(into(list, t))
            assert n > 0

            # Load it again
            into(t, remote,
                 key_filename=os.path.expanduser('~/.ssh/cdh_testing.key'))

            # Doubles length
            assert len(into(list, t)) == 2 * n


def test_hive_resource():
    db = resource('hive://hdfs@%s:10000/default' % host)
    assert isinstance(db, sa.engine.Engine)

    db = resource('hive://%s/' % host)
    assert isinstance(db, sa.engine.Engine)
    assert str(db.url) == 'hive://hdfs@%s:10000/default' % host


def test_append_object_to_HDFS_foo():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    with tmpfile_hdfs('json') as fn:
        js = into('hdfs://%s:%s' % (host, fn), df, hdfs=hdfs)
        assert (into(np.ndarray, js) == into(np.ndarray, df)).all()


def test_dialect_of():
    with filetext(accounts_1_csv) as fn:
        d = dialect_of(CSV(fn))
        assert d['delimiter'] == ','
        assert d['has_header'] is True

    with accounts_data() as (directory, (a, b, c)):
        directory2 = HDFS(Directory(CSV))(directory.path, hdfs=directory.hdfs)
        d = dialect_of(directory2)
        assert d['has_header'] is True

        directory2 = HDFS(Directory(CSV))(directory.path, hdfs=directory.hdfs,
                                          has_header=False)
        d = dialect_of(directory2)
        assert d['has_header'] is False


def test_hive_resource_with_internal_external():
    with hive_table(host) as uri:
        r = resource(uri, external=False, stored_as='PARQUET',
                     dshape='var * {name: string, balance: int32}')
        assert isinstance(r, sa.Table)

    with hive_table(host) as uri:
        r = resource(uri, external=False, stored_as='PARQUET')
        assert not isinstance(r, sa.Table)

    with hive_table(host) as uri:
        r = resource(uri, external=True, stored_as='PARQUET')
        assert not isinstance(r, sa.Table)


def test_copy_hive_csv_table_to_parquet():
    with hive_table(host) as csv:
        with accounts_ssh() as (_, (remote, _, _)):
            c = odo(remote, csv, **auth)
            with hive_table(host) as parquet:
                p = odo(csv, parquet, stored_as='PARQUET', external=False)
                assert odo(c, list) == odo(p, list)

            with hive_table(host) as parquet:
                try:
                    fn = '/home/hdfs/%s.parquet' % str(uuid.uuid1()).replace('-', '')[:20]
                    p = odo(csv, parquet, stored_as='PARQUET',
                            external=True, path=fn)
                    assert odo(c, list) == odo(p, list)
                finally:
                    hdfs.delete_file_dir(fn)
