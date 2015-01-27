import paramiko


from into.utils import tmpfile, filetext, filetexts, raises
from into.backends.ssh import *
import re


def test_resource():
    r = resource('ssh://joe@localhost:/path/to/myfile.csv')
    assert isinstance(r, SSH(CSV))
    assert r.path == '/path/to/myfile.csv'
    assert r.auth['hostname'] == 'localhost'
    assert r.auth['username'] == 'joe'


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
            'user@127.0.0.1:/my-dir/my-file3.csv']
    for uri in uris:
        assert re.match(ssh_pattern, uri)
