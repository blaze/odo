from __future__ import absolute_import, division, print_function
from __future__ import unicode_literals

import sys
import os
import gzip
import bz2
from tempfile import NamedTemporaryFile

from glob import glob
from contextlib import contextmanager

from toolz import concat

import pandas as pd

from dask.threaded import get as dsk_get
import datashape

from datashape import discover, Record, Option
from datashape.dispatch import dispatch

from ..compatibility import unicode
from ..utils import ext, sample, tmpfile
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..temp import Temp
from functools import partial


class ModeProxy(object):
    """Gzipped files use int enums for mode so we want to proxy it to provide
    The proper string version.
    """
    def __init__(self, file, mode):
        self._file = file
        self.mode = mode

    def __getattr__(self, attr):
        return getattr(self._file, attr)

    def __iter__(self):
        return iter(self._file)


@contextmanager
def open_file(path, *args, **kwargs):
    f = compressed_open.get(ext(path), open)(path, *args, **kwargs)
    try:
        yield ModeProxy(f, kwargs.get('mode', 'r'))
    finally:
        f.close()

class NoCloseFile(object):
    def __init__(self, buffer):
        self._buf = buffer

    def __getattr__(self, attr):
        return getattr(self._buf, attr)

    def close(self):
        pass

    def __enter__(self):
        return self._buf

    def __exit__(self, *exc_info):
        pass

    def __iter__(self):
        return iter(self._buf)


class VCF(object):

    """ Proxy for a VCF file

    Parameters
    ----------

    path : str
        Path to file on disk
    kwargs : other...
        Various choices about dialect
    """
    canonical_extension = 'vcf'

    def __init__(self, path=None, buffer=None, **_):
        self.path = path
        self._buffer = buffer

        if path is not None and buffer is not None:
            raise ValueError("may only pass one of 'path' or 'buffer'")

        if path is None and buffer is None:
            raise ValueError("must pass one of 'path' or 'buffer'")

    def open(self, mode='rb', **kwargs):
        buf = self._buffer
        if buf is not None:
            return NoCloseFile(buf)

        return open_file(self.path, mode=mode, **kwargs)

compressed_open = {'gz': gzip.open, 'bz2': bz2.BZ2File}

@convert.register(pd.DataFrame, (Temp(VCF), VCF), cost=20.0)
def vcf_to_dataframe(v, nrows=None, **_):
    with v.open() as f:
        try:
            _skip_meta_data(f)
        except EOFError:
            print("File %s does not look like a VCF one." % v.path)
            raise

        df = pd.read_csv(f, nrows=nrows, sep=str('\t'), encoding='ascii')

    return df

def _skip_meta_data(f):
    line = b" "
    while len(line) > 0:
        last_pos = f.tell()
        line = f.readline()

        if not(len(line) > 1 and line[0:1] == b'#'
               and line[1:2] == b'#'):
            f.seek(last_pos)
            break

@discover.register(VCF)
def discover_vcf(v, nrows=1000, **kwargs):
    df = vcf_to_dataframe(v, nrows=nrows, **kwargs)

    columns = [str(v) if not isinstance(v, (str, unicode)) else v
               for v in df.columns]
    df.columns = [v.strip() for v in columns]

    # Replace np.nan with None. Forces type string rather than float
    for col in df.columns:
        if not df[col].count():
            df[col] = None

    measure = discover(df).measure

    # Use Series.notnull to determine Option-ness
    measure = Record([[name, Option(typ)
                       if df[name].isnull().any() and
                       not isinstance(typ, Option) else typ]
                      for name, typ in zip(measure.names, measure.types)])

    return datashape.var * measure

@resource.register(r'.+\.(vcf)(\.gz|\.bz2?)?')
def resource_vcf(uri, **kwargs):
    return VCF(uri, **kwargs)

@resource.register(r'.*\*.+', priority=12)
def resource_glob(uri, **kwargs):
    filenames = sorted(glob(uri))
    r = resource(filenames[0], **kwargs)
    return chunks(type(r))([resource(u, **kwargs) for u in sorted(glob(uri))])


@convert.register(chunks(pd.DataFrame), (chunks(VCF), chunks(Temp(VCF))),
                  cost=10.0)
def convert_glob_of_vcfs_to_chunks_of_dataframes(vcfs, **kwargs):
    f = partial(convert, chunks(pd.DataFrame), **kwargs)

    def df_gen():
        # build up a dask graph to run all of the `convert` calls concurrently

        # use a list to hold the requested key names to ensure that we return
        # the results in the correct order
        p = []
        dsk = {}
        for n, vcf_ in enumerate(vcfs):
            key = 'p%d' % n
            dsk[key] = f, vcf_
            p.append(key)

        return concat(dsk_get(dsk, p))

    return chunks(pd.DataFrame)(df_gen)

@dispatch(VCF)
def drop(v):
    if v.path is not None:
        os.unlink(v.path)

## IMPLEMENTING

@sample.register(VCF)
@contextmanager
def sample_vcf(vcf, length=8192, **_):
    if vcf.path is None or not os.path.exists(vcf.path):
        mgr = NamedTemporaryFile(suffix='.vcf', mode='wb+')
    else:
        mgr = vcf.open(mode='rb')

    with mgr as f:
        with tmpfile(vcf.canonical_extension) as fn:
            with open(fn, mode='wb') as tmpf:
                tmpf.write(f.read(length))
            yield fn


@append.register(VCF, object)
def append_object_to_vcf(v, seq, **kwargs):
    append(v, convert(chunks(pd.DataFrame), seq, **kwargs), **kwargs)
    return v

@append.register(VCF, pd.DataFrame)
def append_dataframe_to_vcf(v, df, **kwargs):
    if v.path is not None:
        if ext(v.path) in compressed_open:
            if sys.version_info[0] >= 3:
                kwargs['mode'] = 'at'
            elif sys.version_info[0] == 2:
                kwargs['mode'] = 'ab' if sys.platform == 'win32' else 'at'

    with v.open(mode=kwargs.get('mode', 'a')) as f:
        df.to_csv(f,
                  header=None,
                  index=None,
                  sep=str('\t'),
                  encoding='ascii')

    return v


@append.register(VCF, chunks(pd.DataFrame))
def append_iterator_to_vcf(c, cs, **kwargs):
    for chunk in cs:
        append(c, chunk, **kwargs)
    return c

@convert.register(chunks(pd.DataFrame), (Temp(VCF), VCF), cost=10.0)
def VCF_to_chunks_of_dataframes(c, chunksize=2 ** 20, **kwargs):
    # Load a small 1000 line DF to start
    # This helps with rapid viewing of a large VCF file
    first = vcf_to_dataframe(c, nrows=1000, **kwargs)
    if len(first) == 1000:
        rest = vcf_to_dataframe(
            c, chunksize=chunksize, skiprows=1000, **kwargs)
    else:
        rest = []

    data = [first] + rest
    return chunks(pd.DataFrame)(data)


ooc_types.add(VCF)
