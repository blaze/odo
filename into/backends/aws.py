from __future__ import print_function, division, absolute_import

import os
from contextlib import contextmanager

from toolz import memoize
import boto

from into import discover, CSV, resource, convert
from ..utils import mkdir_p, tmpfile

try:
    from urlparse import urlparse
except ImportError:
    from urlllib.parse import urlparse

from multipledispatch import Dispatcher
sample = Dispatcher('sample')


@memoize
def get_s3_connection(aws_access_key_id, aws_secret_access_key, anon=False):
    cfg = boto.Config()

    if aws_access_key_id is None:
        aws_access_key_id = cfg.get('Credentials', 'aws_access_key_id')

    if aws_access_key_id is None:
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')

    if aws_secret_access_key is None:
        aws_secret_access_key = cfg.get('Credentials', 'aws_secret_access_key')

    if aws_secret_access_key is None:
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # anon is False but we didn't provide any credentials so try anonymously
    anon = (not anon and
            aws_access_key_id is None and
            aws_secret_access_key is None)
    return boto.connect_s3(aws_access_key_id, aws_secret_access_key,
                           anon=anon)


class _S3(object):
    def __init__(self, uri, s3=None, aws_access_key_id=None,
                 aws_secret_access_key=None, *args, **kwargs):
        result = urlparse(uri)
        basename, self.key = os.path.split(result.path.lstrip('/'))
        self.bucket = os.path.join(result.netloc, basename).rstrip('/')

        if s3 is not None:
            self.s3 = s3
        else:
            self.s3 = get_s3_connection(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key)
        self.subtype.__init__(self, uri, *args, **kwargs)


@memoize
def S3(cls):
    """Parametrized S3 bucket Class

    Examples
    --------
    >>> import sqlalchemy
    >>> S3(CSV)
    S3(CSV)
    >>> S3(sqlalchemy.Table)
    """
    return type('S3(%s)' % cls.__name__, (_S3, cls), {'subtype': cls})


@resource.register('s3://.*\.csv', priority=18)
def resource_s3_csv(uri):
    return S3(CSV)(uri)


@convert.register(CSV, S3(CSV))
def convert_s3_to_csv(csv, **kwargs):
    """Download an S3 CSV to a local CSV file."""
    bucket = csv.bucket
    mkdir_p(bucket)
    key = csv.s3.get_bucket(bucket).get_key(csv.key)
    filename = os.path.join(bucket, csv.key)
    key.get_contents_to_filename(filename)
    return CSV(filename, **kwargs)


@sample.register(S3(CSV))
@contextmanager
def sample_s3_csv(data, length=8192):
    """Get bytes from `start` to `stop` of `key` from the S3 bucket `bucket`.

    Parameters
    ----------
    data : S3(CSV)
        A CSV file living in  an S3 bucket
    length : int, optional, default ``8192``
        Number of bytes of the file to read

    Examples
    --------
    >>> csv = S3(CSV)('s3://nyqpug/tips.csv')
    >>> print(discover(csv))
    var * {
      total_bill: float64,
      tip: float64,
      sex: string,
      smoker: string,
      day: string,
      time: string,
      size: int64
      }
    """
    # TODO: is Range always accepted by S3?
    # TODO: is Content-Type useful here?
    resp = data.s3.make_request('GET',
                                bucket=data.bucket,
                                key=data.key,
                                headers={'Range': 'bytes=0-%d' % length})
    raw = resp.read()
    raw = raw[:raw.rindex(b'\n')]
    with tmpfile('.csv') as fn:
        with open(fn, 'wb') as f:
            f.write(raw)
        yield fn


@convert.register(S3(CSV), CSV)
def convert_csv_to_s3(csv,
                      bucket_name=None,
                      aws_access_key_id=None,
                      aws_secret_access_key=None,
                      **kwargs):
    assert bucket_name is not None, 'must provide a bucket name'
    s3 = get_s3_connection(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key)
    try:
        bucket = s3.get_bucket(bucket_name)
    except boto.exception.S3ResponseError:  # bucket doesn't exist so create it
        bucket = s3.create_bucket(bucket_name)

    key = boto.s3.key.Key(bucket)
    key.key = os.path.basename(csv.path)

    # TODO: checkout chunked uploads for huge files
    key.set_contents_from_filename(csv.path)
    return S3(CSV('s3://%s/%s' % (bucket.name, key.key)), s3=s3)


def uri_to_path(uri):
    """Turn `uri` into a proper path

    Parameters
    ----------
    s, substring : str

    Returns
    -------
    stripped : str
        `s` without `substring`

    Examples
    --------
    >>> uri_to_path('s3://nyqpug/tips.csv')  # doctest: +SKIP
    'nyqpug/tips.csv'
    >>> uri_to_path('http://nyqpug/tips.csv')  # doctest: +SKIP
    'nyqpug/tips.csv'
    """
    r = urlparse(uri)
    return os.path.join(r.netloc, r.path).replace('/', os.sep)


@discover.register(S3(CSV))
def discover_s3_csv(c, length=8192, **kwargs):
    with sample(c, length=length) as fn:
        return discover(CSV(fn))
