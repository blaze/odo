from __future__ import print_function, division, absolute_import

import os
import tempfile
import gzip
from multiprocessing.pool import ThreadPool

from contextlib import contextmanager

from toolz import memoize, curry, pipe
from toolz.curried import map

import boto
import pandas as pd

from into import discover, CSV, resource, append, convert, drop
from ..utils import tmpfile, ext, sample

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


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


@resource.register('s3://.*\.csv(\.gz)?', priority=18)
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


def upload_part(mp, conn, pair):
    """Upload part of a file to S3

    Parameters
    ----------
    mp : MultiPartUpload
    conn : boto.s3.connection.S3Connection
    pair : tuple of int, string
        The index and filename of the part of the file to upload
    """
    i, filename = pair
    mp_i = boto.s3.multipart.MultiPartUpload(conn.lookup(mp.bucket_name))
    mp_i.key_name = mp.key_name
    mp_i.id = mp.id
    with open(filename, mode='rb') as f:
        mp_i.upload_part_from_file(f, i)
    return filename


_MIN_CHUNK_SIZE = 15 * 1 << 20


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


def pmap(f, iterable):
    """Map `f` over `iterable` in parallel using a ``ThreadPool``.
    """
    p = ThreadPool()
    try:
        result = p.map(f, iterable)
    finally:
        p.terminate()
    return result


def write(triple, writer):
    """Write a file using the input from `gentemp` using `writer` and return
    its index and filename.

    Parameters
    ----------
    triple : tuple of int, str, str
        The first element is the index in the set of chunks of a file, the
        second element is the path to write to, the third element is the data
        to write.

    Returns
    -------
    i, filename : int, str
        File's index and filename. This is used to return the index and
        filename after splitting files.
    """
    i, filename, data = triple
    with writer(filename, mode='wb') as f:
        f.write(data)
    return i, filename


def gentemp(it, suffix=None):
    """Generate an index, a temporary file, and return data for each element
    in `it

    Parameters
    ----------
    it : iterable
    suffix : str
        Suffix to add to each temporary file's name
    """
    for i, data in enumerate(it, start=1):  # aws needs parts to start at 1
        _, fn = tempfile.mkstemp(suffix=suffix)
        yield i, fn, data


def split(filename, nbytes, suffix=None, writer=open):
    """Split a file into chunks of size `nbytes` with each filename containing
    a suffix specified by `suffix`. The file will be written with the ``write``
    method of an instance of `writer`.

    Parameters
    ----------
    filename : str
        The file to split
    nbytes : int
        Split `filename` into chunks of this size
    suffix : str, optional
    writer : callable, optional
        Callable object to use to write the chunks of `filename`
    """
    with open(filename, mode='rb') as f:
        return pmap(curry(write, writer=writer),
                    gentemp(iter(curry(f.read, nbytes), ''), suffix=suffix))


@append.register(_S3, CSV)
def append_csv_to_s3(s3, csv,
                     cutoff=100 * 1 << 20,
                     chunksize=_MIN_CHUNK_SIZE,
                     compression=None,
                     **kwargs):
    if os.path.getsize(csv.path) >= cutoff:  # defaults to 100MB
        if chunksize < _MIN_CHUNK_SIZE:
            raise ValueError('multipart upload with chunksize < %d are not '
                             'allowed by AWS' % _MIN_CHUNK_SIZE)
        if compression is None:
            compression = ext(s3.path)
        if compression == 'gz':
            raise ValueError('gzip compression for multipart uploads not '
                             'implemented')

        # FIXME: implement this properly
        # see http://stackoverflow.com/questions/15754610/how-to-gzip-while-uploading-into-s3-using-boto
        # and https://github.com/wrobstory/pgshift/blob/master/pgshift/pgshift.py#L143
        writer = gzip.GzipFile if compression == 'gz' else open
        with multipart_upload(s3.object) as mp:
            pipe(csv.path,
                 curry(split,
                       nbytes=chunksize,
                       suffix=os.extsep + ext(csv.path),
                       writer=writer),
                 curry(pmap, curry(upload_part, mp, s3.s3)),
                 map(os.remove))
    else:
        # TODO: implement gzip single file uploads
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
