from into.backends.text import TextFile, resource, convert, discover, append
from into.utils import tmpfile, filetext
from datashape import dshape
import os


def test_resource():
    assert isinstance(resource('foo.txt'), TextFile)
    assert isinstance(resource('/path/to/foo.log'), TextFile)


def test_convert():
    with filetext('Hello\nWorld') as fn:
        assert convert(list, TextFile(fn)) == ['Hello\n', 'World']


def test_append():
    with tmpfile('log') as fn:
        t = TextFile(fn)
        append(t, ['Hello', 'World'])

        assert os.path.exists(fn)
        with open(fn) as f:
            assert list(map(str.strip, f.readlines())) == ['Hello', 'World']


def test_discover():
    assert discover(TextFile('')) == dshape('var * string')
