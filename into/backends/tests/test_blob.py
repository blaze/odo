from into.backends.blob import File, resource, drop
import os
from into.utils import filetext


def test_File():
    f = File('/path/to/data')
    assert f.path == '/path/to/data'


def test_resource():
    assert isinstance(resource(os.path.join('path', 'to', 'data.xyz')), File)
    assert isinstance(resource('data.xyz'), File)


def test_drop():
    with filetext('abc', extension='xyz') as fn:
        assert os.path.exists(fn)
        drop(fn)
        assert not os.path.exists(fn)
