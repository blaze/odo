from __future__ import print_function, division, absolute_import

import os
import tempfile

from toolz import memoize
import boto

from into import discover, CSV, resource, convert
from ..utils import mkdir_p


class _S3(object):
    pass


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
    return type('S3(%s)' % cls.__name__, (cls, _S3), {'subtype': cls})


@resource.register('s3://.*\.csv', priority=18)
def resource_s3_csv(uri):
    return S3(CSV)(uri)


@convert.register(CSV, S3(CSV))
def convert_s3_to_csv(csv, **kwargs):
    """Download an S3 CSV to a local CSV file."""
    path = csv.path
    dirname, basename = os.path.split(path)
    dirname = remove_substring(dirname)

    mkdir_p(dirname)

    conn_kwargs = dict(aws_access_key_id=kwargs.pop('aws_access_key_id', None),
                       aws_secret_access_key=(
                                    kwargs.pop('aws_secret_access_key', None)))
    conn = get_s3_connection(**conn_kwargs)
    bucket = conn.get_bucket(dirname)
    key = bucket.get_key(basename)
    filename = os.path.join(dirname, basename)
    key.get_contents_to_filename(filename)
    return CSV(filename, **kwargs)


def get_part(bucket, key, conn=None, start=0, stop=8192,
             aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """Get bytes from `start` to `stop` of `key` from the S3 bucket `bucket`.

    Parameters
    ----------
    bucket : str
        Bucket name
    key : str
        File name
    conn : Connection, optional, default ``None``
        S3 connection
    start : int, optional, default ``0``
        Byte to start at, zero based
    stop : int, optional, default ``8192``
        Byte to stop at, zero based
    aws_access_key_id : str, optional, default ``None``
        Access key id. Will be taken from the environment if ``None``.
    aws_secret_access_key : str, optional, default ``None``
        Secret access key. Will be taken from the environment if ``None``.
    kwargs : dict
        Keyword arguments to pass to the CSV constructor

    Returns
    -------
    csv : CSV
        The ``stop - start`` bytes of the S3 bucket named
        ``s3://<bucket>/<key>`` as a CSV file.

    Examples
    --------
    >>> csv = get_part(bucket='nyqpug', key='tips.csv')
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
    assert stop - start > 0

    if conn is None:
        conn = get_s3_connection(aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)

    # TODO: is Range always accepted by S3?
    # TODO: is Content-Type useful here?
    resp = conn.make_request('GET',
                             bucket=bucket,
                             key=key,
                             headers={'Range': 'bytes=%d-%d' % (start, stop)})
    raw = resp.read()
    raw = raw[:raw.rindex(b'\n')]
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        f.write(raw)
    return CSV(f.name, **kwargs)


@memoize
def get_s3_connection(aws_access_key_id, aws_secret_access_key, anon=False):
    if aws_access_key_id is None:
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')

    if aws_secret_access_key is None:
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # anon is False but we didn't provide any credentials so try anonymously
    anon = (not anon and
            aws_access_key_id is None and
            aws_secret_access_key is None)
    return boto.connect_s3(aws_access_key_id, aws_secret_access_key,
                           anon=anon)


@convert.register(S3(CSV), CSV)
def convert_csv_to_s3(csv,
                      bucket_name=None,
                      aws_access_key_id=None,
                      aws_secret_access_key=None,
                      **kwargs):
    assert bucket_name is not None, 'must provide a bucket name'
    conn = get_s3_connection(aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)
    try:
        bucket = conn.get_bucket(bucket_name)
    except boto.exception.S3ResponseError:  # bucket doesn't exist so create it
        bucket = conn.create_bucket(bucket_name)

    key = boto.s3.key.Key(bucket)
    key.key = os.path.basename(csv.path)

    # TODO: checkout chunked uploads for huge files
    key.set_contents_from_filename(csv.path)
    return S3(CSV('s3://%s/%s' % (bucket.name, key.key)))


def remove_substring(s, substring='s3://'):
    """Remove `substring` from `s`.

    Parameters
    ----------
    s, substring : str

    Returns
    -------
    stripped : str
        `s` without `substring`

    Examples
    --------
    >>> s = 's3://nyqpug/tips.csv'
    >>> remove_substring(s)
    'nyqpug/tips.csv'
    >>> s = 'http://nyqpug/tips.csv'
    >>> remove_substring(s, substring='foo/bar')
    'http://nyqpug/tips.csv'
    """
    return s.replace(substring, '')


@discover.register(S3(CSV))
def discover_s3_csv(c, **kwargs):
    path = remove_substring(c.path)
    bucket, key = os.path.split(path)
    return discover(get_part(bucket=bucket, key=key, **kwargs))
