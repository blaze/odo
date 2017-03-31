from odo.backends.blob import File, resource, drop
import os
from odo.utils import filetext, tmpfile
from odo import odo


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


def test_copy():
    with filetext('abc', extension='xyz') as src:
        with tmpfile('xyz') as tgt:
            odo(src, tgt)
            with open(tgt) as f:
                assert f.read() == 'abc'
