from __future__ import absolute_import, division, print_function

import pytest
paramiko = pytest.importorskip('paramiko')

import pandas as pd
import numpy as np
import re
import os
import sys

from odo.utils import tmpfile, filetext
from odo.directory import _Directory, Directory
from odo.backends.ssh import SSH, resource, ssh_pattern, sftp, drop, connect
from odo.backends.csv import CSV
from odo import into, discover, CSV, JSONLines, JSON, convert
from odo.temp import _Temp, Temp
from odo.compatibility import ON_TRAVIS_CI
import socket

skipif = pytest.mark.skipif

# NOTE: this is a workaround for paramiko on Py2; connect() hangs without
# raising an exception.  Shows up on paramiko 1.16.0 and 2.0.2 with Py 2.7.
# KWS: 2016-08-10
# JJ: Still happens as of 2016-10-20
try_to_connect = sys.version_info[0] >= 3
pytestmark = skipif(not try_to_connect, reason='could not connect')

if try_to_connect:
    try:
        ssh = connect(hostname='localhost')
        ssh.close()
    except socket.error:
        pytestmark = pytest.mark.skip('Could not connect')
    except paramiko.PasswordRequiredException as e:
        pytestmark = pytest.mark.skip(str(e))
    except paramiko.SSHException as e:
        pytestmark = pytest.mark.skip(str(e))
    except TypeError:
        # NOTE: This is a workaround for paramiko version 1.16.0 on Python 3.4,
        # that raises a TypeError due to improper indexing internally into
        # dict_keys when a ConnectionRefused error is raised.
        # KWS 2016-04-21.
        pytestmark = pytest.mark.skip('Could not connect')


def test_resource():
    r = resource('ssh://joe@localhost:/path/to/myfile.csv')
    assert isinstance(r, SSH(CSV))
    assert r.path == '/path/to/myfile.csv'
    assert r.auth['hostname'] == 'localhost'
    assert r.auth['username'] == 'joe'


def test_connect():
    a = connect(hostname='localhost')
    b = connect(hostname='localhost')
    assert a is b

    a.close()

    c = connect(hostname='localhost')
    assert a is c
    assert c.get_transport() and c.get_transport().is_active()


def test_resource_directory():
    r = resource('ssh://joe@localhost:/path/to/')
    assert issubclass(r.subtype, _Directory)

    r = resource('ssh://joe@localhost:/path/to/*.csv')
    assert r.subtype == Directory(CSV)
    assert r.path == '/path/to/'


def test_discover():
    with filetext('name,balance\nAlice,100\nBob,200') as fn:
        local = CSV(fn)
        remote = SSH(CSV)(fn, hostname='localhost')

        assert discover(local) == discover(remote)


def test_discover_from_resource():
    with filetext('name,balance\nAlice,100\nBob,200', extension='csv') as fn:
        local = CSV(fn)
        remote = resource('ssh://localhost:' + fn)

        assert discover(local) == discover(remote)


def test_ssh_pattern():
    uris = ['localhost:myfile.csv',
            '127.0.0.1:/myfile.csv',
            'user@127.0.0.1:/myfile.csv',
            'user@127.0.0.1:/*.csv',
            'user@127.0.0.1:/my-dir/my-file3.csv']
    for uri in uris:
        assert re.match(ssh_pattern, uri)


def test_copy_remote_csv():
    with tmpfile('csv') as target:
        with filetext('name,balance\nAlice,100\nBob,200',
                      extension='csv') as fn:
            csv = resource(fn)

            uri = 'ssh://localhost:%s.csv' % target
            scsv = into(uri, csv)

            assert isinstance(scsv, SSH(CSV))
            assert discover(scsv) == discover(csv)

            # Round trip
            csv2 = into(target, scsv)
            assert into(list, csv) == into(list, csv2)


def test_drop():
    with filetext('name,balance\nAlice,100\nBob,200', extension='csv') as fn:
        with tmpfile('csv') as target:
            scsv = SSH(CSV)(target, hostname='localhost')

            assert not os.path.exists(target)

            conn = sftp(**scsv.auth)
            conn.put(fn, target)

            assert os.path.exists(target)

            drop(scsv)
            drop(scsv)

            assert not os.path.exists(target)


def test_drop_of_csv_json_lines_use_ssh_version():
    from odo.backends.ssh import drop_ssh
    for typ in [CSV, JSON, JSONLines]:
        assert drop.dispatch(SSH(typ)) == drop_ssh


def test_convert_local_file_to_temp_ssh_file():
    with filetext('name,balance\nAlice,100\nBob,200', extension='csv') as fn:
        csv = CSV(fn)
        scsv = convert(Temp(SSH(CSV)), csv, hostname='localhost')

        assert into(list, csv) == into(list, scsv)


@skipif(ON_TRAVIS_CI, reason="Don't know")
def test_temp_ssh_files():
    with filetext('name,balance\nAlice,100\nBob,200', extension='csv') as fn:
        csv = CSV(fn)
        scsv = into(Temp(SSH(CSV)), csv, hostname='localhost')
        assert discover(csv) == discover(scsv)

        assert isinstance(scsv, _Temp)


@skipif(ON_TRAVIS_CI, reason="Don't know")
def test_convert_through_temporary_local_storage():
    with filetext('name,quantity\nAlice,100\nBob,200', extension='csv') as fn:
        csv = CSV(fn)
        df = into(pd.DataFrame, csv)
        scsv = into(Temp(SSH(CSV)), csv, hostname='localhost')

        assert into(list, csv) == into(list, scsv)

        scsv2 = into(Temp(SSH(CSV)), df, hostname='localhost')
        assert into(list, scsv2) == into(list, df)

        sjson = into(Temp(SSH(JSONLines)), df, hostname='localhost')
        assert (into(np.ndarray, sjson) == into(np.ndarray, df)).all()


@skipif(ON_TRAVIS_CI and sys.version_info[0] == 3,
        reason='Strange hanging on travis for python33 and python34')
def test_ssh_csv_to_s3_csv():
    # for some reason this can only be run in the same file as other ssh tests
    # and must be a Temp(SSH(CSV)) otherwise tests above this one fail
    s3_bucket = pytest.importorskip('odo.backends.tests.test_aws').s3_bucket

    with filetext('name,balance\nAlice,100\nBob,200', extension='csv') as fn:
        remote = into(Temp(SSH(CSV)), CSV(fn), hostname='localhost')
        with s3_bucket('.csv') as b:
            result = into(b, remote)
            assert discover(result) == discover(resource(b))


@skipif(ON_TRAVIS_CI and sys.version_info[0] == 3,
        reason='Strange hanging on travis for python33 and python34')
def test_s3_to_ssh():
    pytest.importorskip('boto')

    tips_uri = 's3://nyqpug/tips.csv'
    with tmpfile('.csv') as fn:
        result = into(Temp(SSH(CSV))(fn, hostname='localhost'), tips_uri)
        assert into(list, result) == into(list, tips_uri)
        assert discover(result) == discover(resource(tips_uri))
