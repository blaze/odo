from __future__ import absolute_import, division, print_function

import pytest
import tempfile
from contextlib import contextmanager
import shutil
from odo.backends.csv import CSV
from odo.directory import Directory, discover, resource
from odo import odo
from datashape import dshape
from toolz import concat
import os
import pandas as pd


@contextmanager
def csvs(n=3):
    path = tempfile.mktemp()
    os.mkdir(path)

    fns = [os.path.join(path, 'file_%d.csv' % i) for i in range(n)]

    for i, fn in enumerate(fns):
        odo([{'a': i, 'b': j} for j in range(5)], fn)

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


def test_resource_nonexistent_directory():
    with pytest.raises(AssertionError):
        resource(os.path.join('a', 'nonexistent', 'directory') + os.path.sep)


def test_directory_of_csvs_to_frame():
    with csvs() as path:
        result = odo(odo(path, pd.DataFrame), set)
        expected = odo(concat(odo(fn, list) for fn in resource(path)), set)
    assert result == expected
