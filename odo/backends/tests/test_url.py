from __future__ import print_function

import pytest

import os

from odo import into, resource, URL, discover, CSV, TextFile
from odo.utils import tmpfile

import datashape

try:
    from urllib2 import urlopen
    from urllib2 import HTTPError, URLError
except ImportError:
    from urllib.request import urlopen
    from urllib.error import HTTPError, URLError

skipif = pytest.mark.skipif

try:
    r = urlopen("http://google.com")
except URLError:
    pytest.skip('Could not connect')


iris_url = 'https://raw.githubusercontent.com/ContinuumIO/blaze/master/blaze/examples/data/iris.csv'
ftp_url = "ftp://athena-dist.mit.edu/pub/XNeXT/README.txt"

def test_url_resource():
    csv = resource(iris_url)
    assert isinstance(csv, URL(CSV))


@pytest.mark.xfail(raises=HTTPError)
def test_failed_url():
    failed_url = "http://foo.com/myfile.csv"
    with tmpfile('.csv') as fn:
        into(fn, failed_url)

def test_url_discover():
    csv = resource(iris_url)
    assert isinstance(discover(csv), datashape.DataShape)


def test_url_to_local_csv():
    with tmpfile('.csv') as fn:
        csv = into(fn, iris_url)
        path = os.path.abspath(csv.path)
        assert os.path.exists(path)

def test_url_txt_resource():
    txt = resource(ftp_url)
    assert isinstance(txt, URL(TextFile))


def test_ftp_to_local_txt():
    with tmpfile('.txt') as fn:
        txt = into(fn, ftp_url)
        path = os.path.abspath(txt.path)
        assert os.path.exists(path)
