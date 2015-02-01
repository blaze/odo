from __future__ import print_function, division, absolute_import

import os
import uuid

from contextlib import contextmanager

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import pandas as pd
from toolz import memoize, curry, pipe
from toolz.curried import map

import boto

from into import discover, CSV, resource, append, convert, drop, Temp, JSON
from into import JSONLines, SSH, into

from ..utils import tmpfile, ext, sample
from ..utils import pmap, split


@memoize
def get_s3_connection(aws_access_key_id=None,
                      aws_secret_access_key=None,
                      anon=False):
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
    headers = {'Range': 'bytes=0-%d' % length}
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


@discover.register(S3(CSV))
def discover_s3_csv(c, length=8192, **kwargs):
    with sample(c, length=length) as fn:
        return discover(CSV(fn, **kwargs), **kwargs)


@resource.register('s3://.*\.(csv|txt)', priority=18)
def resource_s3_csv(uri, **kwargs):
    return S3(CSV)(uri, **kwargs)


@resource.register('s3://.*\.json', priority=18)
def resource_s3_csv(uri, **kwargs):
    return S3(JSON)(uri, **kwargs)


@drop.register((S3(CSV), S3(JSON), S3(JSONLines)))
def drop_s3_csv(s3):
    s3.object.delete()


@convert.register(Temp(CSV), (Temp(S3(CSV)), S3(CSV)))
@convert.register(Temp(JSON), (Temp(S3(JSON)), S3(JSON)))
@convert.register(Temp(JSONLines), (Temp(S3(JSONLines)), S3(JSONLines)))
def s3_csv_to_temp_csv(s3, **kwargs):
    tmp_filename = '.%s.%s' % (uuid.uuid1(), ext(s3.path))
    s3.object.get_contents_to_filename(tmp_filename)
    return Temp(s3.subtype)(tmp_filename, **kwargs)


@convert.register(Temp(S3(CSV)), CSV)
@convert.register(Temp(S3(JSON)), JSON)
@convert.register(Temp(S3(JSONLines)), JSONLines)
def text_data_to_temp_s3_text_data(data, **kwargs):
    subtype = type(data)
    uri = 's3://%s/%s.%s' % (uuid.uuid1(), uuid.uuid1(), ext(data.path))
    into(uri, data)
    return Temp(S3(subtype))(uri, **kwargs)


@append.register(S3(CSV), pd.DataFrame)
def frame_to_s3_csv(s3, df, **kwargs):
    return into(s3, into(Temp(CSV), df, **kwargs), **kwargs)


@append.register(S3(JSONLines), (JSONLines, Temp(JSONLines)))
@append.register(S3(JSON), (JSON, Temp(JSON)))
@append.register(S3(CSV), (CSV, Temp(CSV)))
def append_csv_to_s3(s3, data, **kwargs):
    s3.object.set_contents_from_filename(data.path)
    return s3


@append.register(S3(JSON), SSH(JSON))
@append.register(S3(JSONLines), SSH(JSONLines))
@append.register(S3(CSV), SSH(CSV))
def ssh_text_to_s3_text(a, b, **kwargs):
    # TODO: might be able to do this without a temporary local file
    # the host would need to have
    return into(a, into(Temp(b.subtype), b, **kwargs), **kwargs)
