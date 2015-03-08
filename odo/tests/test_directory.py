from __future__ import absolute_import, division, print_function

import tempfile
from contextlib import contextmanager
import shutil
from odo.backends.csv import CSV
from odo.directory import Directory, discover, resource, _Directory
from odo import into
from datashape import dshape
import os

@contextmanager
def csvs(n=3):
    path = tempfile.mktemp()
    os.mkdir(path)

    fns = [os.path.join(path, 'file_%d.csv' % i) for i in range(n)]

    for i, fn in enumerate(fns):
        into(fn, [{'a': i, 'b': j} for j in range(5)])

    try:
        yield path + os.path.sep
    finally:
        shutil.rmtree(path)


def test_discover():
    with csvs() as path:
        d = Directory(CSV)(path)
        assert discover(d) == dshape('var * {a: int64, b: int64}')


def test_resource_directory():
    with csvs() as path:
        r = resource(path)
        assert type(r) == Directory(CSV)
        assert r.path.rstrip(os.path.sep) == path.rstrip(os.path.sep)

        r2 = resource(os.path.join(path, '*.csv'))
        assert type(r2) == Directory(CSV)
        assert r2.path.rstrip(os.path.sep) == path.rstrip(os.path.sep)


def test_resource_directory():
    assert isinstance(resource(os.path.join('a', 'nonexistent', 'directory') +
                               os.path.sep),
                      _Directory)
