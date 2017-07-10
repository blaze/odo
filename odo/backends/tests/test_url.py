from __future__ import print_function

import pytest
from mock import patch, Mock

from functools import partial
import codecs
import os

from odo import odo, resource, URL, discover, CSV, TextFile, convert
from odo.backends.url import sample
from odo.temp import _Temp, Temp
from odo.utils import tmpfile, raises

import datashape

try:
    from urllib2 import urlopen
    from urllib2 import HTTPError, URLError
except ImportError:
    from urllib.request import urlopen
    from urllib.error import HTTPError, URLError

pytestmark = pytest.mark.skipif(raises(URLError,
                                       partial(urlopen, "http://google.com")),
                                reason='unable to connect to google.com')

iris_url = ('https://raw.githubusercontent.com/'
            'blaze/blaze/master/blaze/examples/data/iris.csv')
ftp_url = "ftp://athena-dist.mit.edu/pub/XNeXT/README.txt"


def test_url_resource():
    csv = resource(iris_url)
    assert isinstance(csv, URL(CSV))


def test_small_chunk_size():
    normal = convert(Temp(CSV), resource(iris_url))
    small_chunk = convert(Temp(CSV), resource(iris_url, chunk_size=1))
    with open(normal.path, 'rb') as fn:
        normal_data = fn.read()
    with open(small_chunk.path, 'rb') as fn:
        small_chunk_data = fn.read()
    assert normal_data == small_chunk_data



def test_sample_different_line_counts():
    with sample(resource(iris_url), lines=10) as fn:
        with open(fn, 'r') as f:
            assert len(list(f)) == 10

    with sample(resource(iris_url), lines=5) as fn:
        with open(fn, 'r') as f:
            assert len(list(f)) == 5


def test_sample_different_encoding():
    encoding = 'latin-1'
    lines = 10
    with sample(resource(iris_url), lines=lines, encoding=encoding) as fn:
        with codecs.open(fn, 'r', encoding=encoding) as f:
            assert len(list(f)) == lines


@pytest.mark.xfail(raises=HTTPError)
@patch('odo.backends.url.urlopen')
def test_failed_url(m):
    failed_url = "http://foo.com/myfile.csv"
    m.side_effect = HTTPError(failed_url, 404, 'Not found', None, None)
    with tmpfile('.csv') as fn:
        odo(failed_url, fn)


def test_url_discover():
    csv = resource(iris_url)
    assert isinstance(discover(csv), datashape.DataShape)


def test_url_to_local_csv():
    with tmpfile('.csv') as fn:
        csv = odo(iris_url, fn)
        path = os.path.abspath(csv.path)
        assert os.path.exists(path)


def test_url_txt_resource():
    txt = resource(ftp_url)
    assert isinstance(txt, URL(TextFile))


@pytest.mark.xfail(
    raises=URLError,
    reason='MIT Athena FTP is down as of October 23, 2015'
)
def test_ftp_to_local_txt():
    with tmpfile('.txt') as fn:
        txt = odo(ftp_url, fn, timeout=5)
        path = os.path.abspath(txt.path)
        assert os.path.exists(path)


def test_convert():
    url_csv = resource(iris_url)
    t_csv = convert(Temp(CSV), url_csv)
    assert discover(url_csv) == discover(t_csv)

    assert isinstance(t_csv, _Temp)


@pytest.mark.skipif(os.environ.get('HDFS_TEST_HOST') is None,
                    reason='No HDFS_TEST_HOST envar defined')
def test_url_to_hdfs():
    from .test_hdfs import tmpfile_hdfs, hdfs, HDFS

    with tmpfile_hdfs() as target:

        # build temp csv for assertion check
        url_csv = resource(iris_url)
        csv = convert(Temp(CSV), url_csv)

        # test against url
        scsv = HDFS(CSV)(target, hdfs=hdfs)
        odo(iris_url, scsv)

        assert discover(scsv) == discover(csv)
