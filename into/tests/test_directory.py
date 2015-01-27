from __future__ import absolute_import, division, print_function

import tempfile
from contextlib import contextmanager
import shutil
from into.backends.csv import CSV
from into.directory import Directory, discover
from into import into
from datashape import dshape
import os

@contextmanager
def csvs(n=3):
    path = tempfile.mktemp()
    os.mkdir(path)

    fns = ['%s/file_%d.csv' % (path, i) for i in range(n)]

    for i, fn in enumerate(fns):
        into(fn, [{'a': i, 'b': j} for j in range(5)])

    try:
        yield path
    finally:
        shutil.rmtree(path)


def test_discover():
    with csvs() as path:
        d = Directory(CSV)(path)
        assert discover(d) == dshape('var * {a: int64, b: int64}')
