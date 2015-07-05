from __future__ import absolute_import, division, print_function

import pytest
import tempfile
from contextlib import contextmanager
import shutil
from odo.backends.csv import CSV
from odo.directory import Directory, discover, resource, _Directory
from odo import odo
from datashape import dshape
from toolz import concat
import os
import pandas as pd


@contextmanager
def csvs(n=3):
    assert 0 < n <= 10, \
        'number of files must be greater than 0 and less than or equal to 10'
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


@contextmanager
def chdir(newdir):
    curdir = os.getcwd()
    os.chdir(newdir)

    try:
        yield os.getcwd()
    finally:
        os.chdir(curdir)


def test_directory_of_csvs_to_frame():
    with csvs() as path:
        result = odo(odo(path, pd.DataFrame), set)
        expected = odo(concat(odo(fn, list) for fn in resource(path)), set)
    assert result == expected


def test_directory_of_csvs_with_cd():
    with csvs() as path:
        with chdir(path):
            result = odo(odo('./*.csv', pd.DataFrame), set)
        expected = odo(concat(odo(fn, list) for fn in resource(path)), set)
    assert result == expected


def test_directory_of_csvs_with_non_star_glob():
    with csvs() as path:
        with chdir(path):
            glob_pat = '%sfile_?.csv' % path
            dir_of_csv = resource(glob_pat)
            assert isinstance(dir_of_csv, _Directory)
            result = odo(odo(dir_of_csv, pd.DataFrame), set)
        expected = odo(concat(odo(fn, list) for fn in resource(path)), set)
    assert result == expected
