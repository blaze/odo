from __future__ import print_function, division, absolute_import

import os
from contextlib import contextmanager

from toolz import memoize
import boto
import pandas as pd

from into import discover, CSV, resource, append, convert, drop
from ..utils import tmpfile

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

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
        self.bucket = result.netloc
        self.key = result.path.lstrip('/')

        if s3 is not None:
            self.s3 = s3
        else:
            self.s3 = get_s3_connection(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key)
        try:
            bucket = self.s3.get_bucket(self.bucket)
        except boto.exception.S3ResponseError:
            bucket = self.s3.create_bucket(self.bucket)

        self.object = bucket.get_key(self.key)
        if self.object is None:
            self.object = bucket.new_key(self.key)

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
def resource_s3_csv(uri, **kwargs):
    return S3(CSV)(uri, **kwargs)


@append.register(CSV, S3(CSV))
def append_s3_to_csv(csv, s3csv, **kwargs):
    """Download an S3 CSV to a local CSV file."""
    s3csv.object.get_contents_to_filename(csv.path)
    return append(csv, CSV(csv.path, **kwargs))


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
    headers = {'Range': 'bytes=0-%d' % length}#, 'Content-Type': 'text/csv'}
    raw = data.object.get_contents_as_string(headers=headers)

    # this is generally cheap as we usually have a tiny amount of data
    try:
        index = raw.rindex(b'\r\n')
    except ValueError:
        index = raw.rindex(b'\n')

    raw = raw[:index]

    with tmpfile('.csv') as fn:
        # we use wb because without an encoding boto returns bytes
        with open(fn, 'wb') as f:
            f.write(raw)
        yield fn


@append.register(_S3, CSV)
def append_csv_to_s3(s3, csv, **kwargs):
    # TODO: checkout chunked uploads for huge files
    s3.object.set_contents_from_filename(csv.path)
    return s3


@discover.register(S3(CSV))
def discover_s3_csv(c, length=8192, **kwargs):
    with sample(c, length=length) as fn:
        return discover(CSV(fn, **kwargs), **kwargs)


@convert.register(pd.DataFrame, S3(CSV))
def s3_csv_to_frame(csv, **kwargs):
    # yes, this is an anti-pattern except that if we only define append, then
    # we need a way into the convert graph
    return convert(pd.DataFrame, CSV(csv.path, **kwargs))


@drop.register(S3(CSV))
def drop_s3_csv(s3):
    s3.object.delete()
