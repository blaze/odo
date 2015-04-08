from __future__ import print_function

import pytest
import sys

import os

from odo import into, resource, URL, discover, CSV, drop, append
from odo.utils import tmpfile


import datashape

iris_url = 'https://raw.githubusercontent.com/ContinuumIO/blaze/master/blaze/examples/data/iris.csv'

def test_url_resource():
    csv = resource(iris_url)
    assert isinstance(csv, URL(CSV))


def test_url_discover():
    csv = resource(iris_url)
    assert isinstance(discover(csv), datashape.DataShape)


def test_url_to_local_csv():
    with tmpfile('.csv') as fn:
        csv = into(fn, iris_url)
        path = os.path.abspath(csv.path)
        assert os.path.exists(path)
