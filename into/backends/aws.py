from __future__ import print_function, division, absolute_import

import os
import uuid
import atexit

from contextlib import contextmanager

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from toolz import memoize, curry, pipe

import boto

from into import discover, CSV, resource, append, convert, drop, Temp, JSON
from into import JSONLines
from into.backends.temp import _Temp
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


@resource.register('s3://.*\.(csv|json)?', priority=18)
def resource_s3_csv(uri, **kwargs):
    return S3(CSV)(uri, **kwargs)


@drop.register(S3(CSV))
def drop_s3_csv(s3):
    s3.object.delete()


# @append.register(CSV, S3(CSV))
# def append_s3_to_csv(csv, s3csv, **kwargs):
#     """Download an S3 CSV to a local CSV file."""
#     s3csv.object.get_contents_to_filename(csv.path)
#     return append(csv, CSV(csv.path, **kwargs))


@convert.register(Temp(_S3), (JSONLines, JSON, CSV))
def s3_text_data_to_temp_csv(data, **kwargs):
    return Temp(S3(type(data)))('s3://%s/%s.%s' %
                                (tmpbucket, uuid.uuid1(), ext(data.path)))


@convert.register(_Temp, _S3)
def s3_csv_to_temp_csv(s3, **kwargs):
    tmp_filename = '.%s.%s' % (uuid.uuid1(), ext(s3.path))
    s3.object.get_contents_to_filename(tmp_filename)
    return Temp(s3.subtype)(tmp_filename)


_MIN_CHUNK_SIZE = 15 * 1 << 20


@append.register(_S3, (JSONLines, JSON, CSV))
def append_csv_to_s3(s3, csv,
                     cutoff=100 * 1 << 20,
                     chunksize=_MIN_CHUNK_SIZE,
                     **kwargs):
    if os.path.getsize(csv.path) >= cutoff:  # defaults to 100MB
        if chunksize < _MIN_CHUNK_SIZE:
            raise ValueError('multipart upload with chunksize < %d are not '
                             'allowed by AWS' % _MIN_CHUNK_SIZE)
        # TODO: implement gzip
        with multipart_upload(s3.object) as mp:
            pipe(csv.path,
                 split(nbytes=chunksize, suffix=os.extsep + ext(csv.path),
                       start=1),
                 pmap(upload_part(mp)),
                 pmap(os.remove))
    else:
        # TODO: implement gzip single file uploads
        s3.object.set_contents_from_filename(csv.path)
    return s3


@curry
def upload_part(mp, pair):
    """Upload part of a file to S3

    Parameters
    ----------
    mp : MultiPartUpload
    conn : boto.s3.connection.S3Connection
    pair : tuple of int, string
        The index and filename of the part of the file to upload
    """
    i, filename = pair
    with open(filename, mode='rb') as f:
        mp.upload_part_from_file(f, i)
    return filename


@contextmanager
def multipart_upload(key):
    """Context manager to yield a multipart upload object for use in uploading
    large files to S3

    Parameters
    ----------
    key : boto.s3.key.Key
        Key object to upload in multiple parts
    """
    mp = key.bucket.initiate_multipart_upload(key.name)
    yield mp
    mp.complete_upload()


# make a temporary bucket for this interpreter instance
tmpbucket = str(uuid.uuid1())


@atexit.register
def delete_global_tmpbucket():
    try:
        conn = get_s3_connection()
    except Exception as e:
        print(e)
    else:
        bucket = conn.lookup(tmpbucket)
        if bucket is not None:
            bucket.delete()
