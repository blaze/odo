from __future__ import print_function, division, absolute_import

import os
import uuid
import zlib
import re
from fnmatch import fnmatch
from contextlib import contextmanager
from collections import Iterator
from operator import attrgetter
from io import BytesIO

import pandas as pd
from toolz import memoize, first, concat, curry

from .. import (discover, CSV, resource, append, convert, drop, Temp, JSON,
                JSONLines, chunks)

from multipledispatch import MDNotImplementedError

from .text import TextFile

from ..compatibility import urlparse
from ..utils import tmpfile, ext, sample, filter_kwargs, copydoc


@memoize
def get_s3_connection(aws_access_key_id=None, aws_secret_access_key=None,
                      anon=False, profile_name=None, **kwargs):
    import boto

    if profile_name:
        return boto.connect_s3(profile_name=profile_name)

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
    """Parametrized S3 bucket Class

    Examples
    --------
    >>> S3(CSV)
    <class 'odo.backends.aws.S3(CSV)'>
    """
    def __init__(self, uri, s3=None, aws_access_key_id=None,
                 aws_secret_access_key=None, *args, **kwargs):
        import boto
        result = urlparse(uri)
        self.bucket = result.netloc
        self.key = result.path.lstrip('/')

        if s3 is not None:
            self.s3 = s3
        else:
            self.s3 = get_s3_connection(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        **kwargs)
        try:
            bucket = self.s3.get_bucket(self.bucket,
                                        validate=False,
                                        **filter_kwargs(self.s3.get_bucket,
                                                        kwargs))
        except boto.exception.S3ResponseError:
            bucket = self.s3.create_bucket(self.bucket,
                                           **filter_kwargs(self.s3.create_bucket,
                                                           kwargs))

        self.object = bucket.get_key(self.key, **filter_kwargs(bucket.get_key,
                                                               kwargs))
        if self.object is None:
            self.object = bucket.new_key(self.key)

        self.subtype.__init__(self, uri, *args,
                              **filter_kwargs(self.subtype.__init__, kwargs))


@memoize
@copydoc(_S3)
def S3(cls):
    return type('S3(%s)' % cls.__name__, (_S3, cls), {'subtype': cls})


@sample.register((S3(CSV), S3(JSONLines)))
@contextmanager
def sample_s3_line_delimited(data, length=8192):
    """Get a size `length` sample from an S3 CSV or S3 line-delimited JSON.

    Parameters
    ----------
    data : S3(CSV)
        A CSV file living in an S3 bucket
    length : int, optional, default ``8192``
        Number of bytes of the file to read
    """
    headers = {'Range': 'bytes=0-%d' % length}
    if data.object.exists():
        key = data.object
    else:  # we are probably trying to read from a set of files
        keys = sorted(data.object.bucket.list(prefix=data.object.key),
                      key=attrgetter('key'))
        if not keys:
            # we didn't find anything with a prefix of data.object.key
            raise ValueError('Object %r does not exist and no keys with a '
                             'prefix of %r exist' %
                             (data.object, data.object.key))
        key = first(keys)
    raw = key.get_contents_as_string(headers=headers)

    if ext(key.key) == 'gz':
        # decompressobj allows decompression of partial streams
        raw = zlib.decompressobj(32 + zlib.MAX_WBITS).decompress(raw)

    # this is generally cheap as we usually have a tiny amount of data
    try:
        index = raw.rindex(b'\r\n')
    except ValueError:
        index = raw.rindex(b'\n')

    raw = raw[:index]

    with tmpfile(ext(re.sub(r'\.gz$', '', data.path))) as fn:
        # we use wb because without an encoding boto returns bytes
        with open(fn, 'wb') as f:
            f.write(raw)
        yield fn


@discover.register((S3(CSV), S3(JSONLines)))
def discover_s3_line_delimited(c, length=8192, **kwargs):
    """Discover CSV and JSONLines files from S3."""
    with sample(c, length=length) as fn:
        return discover(c.subtype(fn, **kwargs), **kwargs)


@resource.register('s3://.*\.csv(\.gz)?', priority=18)
def resource_s3_csv(uri, **kwargs):
    return S3(CSV)(uri, **kwargs)


@resource.register('s3://.*\*.csv(\.gz)?', priority=19)
def resource_s3_csv_glob(uri, **kwargs):
    con = get_s3_connection(**kwargs)
    result = urlparse(uri)
    bucket = con.get_bucket(result.netloc, validate=False)
    key = result.path.lstrip('/')

    all_keys = bucket.list(prefix=key.split('*')[0])
    matched = [k for k in all_keys if fnmatch(k.key, key)]
    uris = ['s3://{}/{}'.format(bucket.name, k.key) for k in matched]
    r = resource(uris[0], **kwargs)
    return chunks(type(r))([resource(u, **kwargs) for u in sorted(uris)])


@convert.register(chunks(pd.DataFrame), chunks(S3(CSV)), cost=11.0)
def convert_gob_of_s3_csvs_to_chunks_of_dataframes(csvs, **kwargs):
   def _():
       return concat(convert(chunks(pd.DataFrame), csv, **kwargs)
                      for csv in csvs)
   return chunks(pd.DataFrame)(_)


@resource.register('s3://.*\.txt(\.gz)?', priority=18)
def resource_s3_text(uri, **kwargs):
    return S3(TextFile)(uri)


@resource.register('s3://.*\.json(\.gz)?', priority=18)
def resource_s3_json_lines(uri, **kwargs):
    return S3(JSONLines)(uri, **kwargs)


@drop.register((S3(CSV), S3(JSON), S3(JSONLines), S3(TextFile)))
def drop_s3(s3_object):
    s3_object.object.delete()


@drop.register((Temp(S3(CSV)), Temp(S3(JSON)), Temp(S3(JSONLines)),
                Temp(S3(TextFile))))
def drop_temp_s3(s3_object):
    s3_object.object.delete()
    s3_object.object.bucket.delete()


@convert.register(Temp(CSV), (Temp(S3(CSV)), S3(CSV)))
@convert.register(Temp(JSON), (Temp(S3(JSON)), S3(JSON)))
@convert.register(Temp(JSONLines), (Temp(S3(JSONLines)), S3(JSONLines)))
@convert.register(Temp(TextFile), (Temp(S3(TextFile)), S3(TextFile)))
def s3_text_to_temp_text(s3_object, **kwargs):
    tmp_filename = '.%s.%s' % (uuid.uuid1(), ext(s3_object.path))
    s3_object.object.get_contents_to_filename(tmp_filename)
    return Temp(s3_object.subtype)(tmp_filename, **kwargs)


@append.register(CSV, S3(CSV))
@append.register(JSON, S3(JSON))
@append.register(JSONLines, S3(JSONLines))
@append.register(TextFile, S3(TextFile))
def s3_text_to_text(data, s3_object, **kwargs):
    s3_object.object.get_contents_to_filename(data.path)
    return data


@append.register((S3(CSV), Temp(S3(CSV))), (S3(CSV), Temp(S3(CSV))))
@append.register((S3(JSON), Temp(S3(JSON))), (S3(JSON), Temp(S3(JSON))))
@append.register((S3(JSONLines), Temp(S3(JSONLines))),
                 (S3(JSONLines), Temp(S3(JSONLines))))
@append.register((S3(TextFile), Temp(S3(TextFile))),
                 (S3(TextFile), Temp(S3(TextFile))))
def temp_s3_to_s3(a, b, **kwargs):
    a.object.bucket.copy_key(b.object.name, a.object.bucket.name,
                             b.object.name)
    return a


@convert.register(Temp(S3(CSV)), (CSV, Temp(CSV)))
@convert.register(Temp(S3(JSON)), (JSON, Temp(JSON)))
@convert.register(Temp(S3(JSONLines)), (JSONLines, Temp(JSONLines)))
@convert.register(Temp(S3(TextFile)), (TextFile, Temp(TextFile)))
def text_to_temp_s3_text(data, **kwargs):
    subtype = getattr(data, 'persistent_type', type(data))
    uri = 's3://%s/%s.%s' % (uuid.uuid1(), uuid.uuid1(), ext(data.path))
    return append(Temp(S3(subtype))(uri, **kwargs), data)


@append.register((S3(CSV), S3(JSON), S3(JSONLines), S3(TextFile)),
                 (pd.DataFrame, chunks(pd.DataFrame), (list, Iterator)))
def anything_to_s3_text(s3_object, o, **kwargs):
    return append(s3_object, convert(Temp(s3_object.subtype), o, **kwargs), **kwargs)


@contextmanager
def start_multipart_upload_operation(s3_object, **kwargs):
    multipart_upload = s3_object.object.bucket.initiate_multipart_upload(s3_object.key, **kwargs)

    try:
        yield multipart_upload
    except Exception:
        for part in multipart_upload:
            s3_object.object.bucket.cancel_multipart_upload(part.key_name, part.id)
        raise
    else:
        multipart_upload.complete_upload()


@append.register(S3(JSONLines), (JSONLines, Temp(JSONLines)))
@append.register(S3(JSON), (JSON, Temp(JSON)))
@append.register(S3(CSV), (CSV, Temp(CSV)))
@append.register(S3(TextFile), (TextFile, Temp(TextFile)))
def append_text_to_s3(s3_object, data, multipart=False, part_size=5 << 20, **kwargs):
    if multipart:
        filtered_kwargs = filter_kwargs(s3_object.object.bucket.initiate_multipart_upload, kwargs)
        with start_multipart_upload_operation(s3_object, **filtered_kwargs) as multipart_upload:
            with open(data.path, 'rb') as f:
                parts = enumerate(iter(curry(f.read, part_size), ''), start=1)
                for part_num, part in parts:
                    multipart_upload.upload_part_from_file(
                        BytesIO(part),
                        part_num=part_num
                    )
        return s3_object

    s3_object.object.set_contents_from_filename(
        data.path, **filter_kwargs(s3_object.object.set_contents_from_filename, kwargs))
    return s3_object


try:
    from .hdfs import HDFS
except ImportError:
    pass
else:
    @append.register(S3(JSON), HDFS(JSON))
    @append.register(S3(JSONLines), HDFS(JSONLines))
    @append.register(S3(CSV), HDFS(CSV))
    @append.register(S3(TextFile), HDFS(TextFile))
    @append.register(HDFS(JSON), S3(JSON))
    @append.register(HDFS(JSONLines), S3(JSONLines))
    @append.register(HDFS(CSV), S3(CSV))
    @append.register(HDFS(TextFile), S3(TextFile))
    def other_remote_text_to_s3_text(a, b, **kwargs):
        raise MDNotImplementedError()


try:
    from .ssh import connect, _SSH, SSH
except ImportError:
    pass
else:
    @append.register(S3(JSON), SSH(JSON))
    @append.register(S3(JSONLines), SSH(JSONLines))
    @append.register(S3(CSV), SSH(CSV))
    @append.register(S3(TextFile), SSH(TextFile))
    def remote_text_to_s3_text(a, b, **kwargs):
        return append(a, convert(Temp(b.subtype), b, **kwargs), **kwargs)


    @append.register(_SSH, _S3)
    def s3_to_ssh(ssh, s3_object, url_timeout=600, **kwargs):
        if s3_object.s3.anon:
            url = 'https://%s.s3.amazonaws.com/%s' % (s3_object.bucket, s3_object.object.name)
        else:
            url = s3_object.object.generate_url(url_timeout)
        command = "wget '%s' -qO- >> '%s'" % (url, ssh.path)
        conn = connect(**ssh.auth)
        _, stdout, stderr = conn.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status:
            raise ValueError('Error code %d, message: %r' % (exit_status,
                                                             stderr.read()))
        return ssh
