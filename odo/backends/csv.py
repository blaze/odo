from __future__ import absolute_import, division, print_function

import sys
import re
import os
import gzip
import bz2
import uuid
import csv

from glob import glob
from contextlib import contextmanager

from toolz import concat, keyfilter, keymap, merge, valfilter

import pandas as pd

import datashape

from datashape import discover, Record, Option
from datashape.predicates import isrecord
from datashape.dispatch import dispatch

from ..compatibility import unicode, PY2
from ..utils import keywords, ext, sample, tmpfile
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..temp import Temp
from ..numpy_dtype import dshape_to_pandas
from .pandas import coerce_datetimes

dialect_terms = '''delimiter doublequote escapechar lineterminator quotechar
quoting skipinitialspace strict'''.split()

aliases = {'sep': 'delimiter'}


class PipeSniffer(csv.Sniffer):
    """A csv sniffer that adds the ``'|'`` character to the list of preferred
    delimiters and gives space the least precedence.

    Notes
    -----
    The :module:`~csv` module has a list of preferred delimiters that will be
    returned before the delimiter(s) discovered by parsing the file. This
    results in a dialect having an incorrect delimiter if the data contain a
    non preferred delimiter and a preferred one. See the Examples section for
    an example showing the difference between the two sniffers.

    Examples
    --------
    >>> import csv
    >>> data = 'a|b| c|d'  # space is preferred but '|' is more common
    >>> csv.Sniffer().sniff(data).delimiter  # incorrect
    ' '
    >>> PipeSniffer().sniff(data).delimiter  # correct
    '|'
    >>> data = 'a|b|Nov 10, 1981|d'
    >>> csv.Sniffer().sniff(data).delimiter  # can't handle every case :(
    ','
    """
    def __init__(self, *args, **kwargs):
        csv.Sniffer.__init__(self, *args, **kwargs)
        self.preferred = [',', '\t', ';', '|', ':', ' ']


def alias(key):
    """ Alias kwarg dialect keys to normalized set

    >>> alias('sep')
    'delimiter'
    """
    key = key.lower()
    return aliases.get(key, key)


@contextmanager
def open_file(path, *args, **kwargs):
    f = compressed_open.get(ext(path), open)(path, *args, **kwargs)
    try:
        yield f
    finally:
        f.close()


def infer_header(path, nbytes=10000, encoding='utf-8', **kwargs):
    if encoding is None:
        encoding = 'utf-8'
    with open_file(path, 'rb') as f:
        raw = f.read(nbytes)
    if not raw:
        return True
    sniffer = PipeSniffer()
    decoded = raw.decode(encoding, 'replace')
    sniffable = decoded.encode(encoding) if PY2 else decoded
    try:
        return sniffer.has_header(sniffable)
    except csv.Error:
        return None


def newlines(encoding):
    return b'\r\n'.decode(encoding), b'\n'.decode(encoding)


def sniff_dialect(path, nbytes, encoding='utf-8'):
    if not os.path.exists(path):
        return {}
    if encoding is None:
        encoding = 'utf-8'
    with open_file(path, 'rb') as f:
        raw = f.read(nbytes).decode(encoding or 'utf-8', 'replace')
    sniffer = PipeSniffer()
    try:
        dialect = sniffer.sniff(raw, delimiters=sniffer.preferred)
    except csv.Error:
        return {}
    crnl, nl = newlines(encoding)
    dialect.lineterminator = crnl if crnl in raw else nl
    return dialect_to_dict(dialect)


def dialect_to_dict(dialect):
    return dict((name, getattr(dialect, name))
                for name in dialect_terms if hasattr(dialect, name))


class CSV(object):

    """ Proxy for a CSV file

    Parameters
    ----------

    path : str
        Path to file on disk
    has_header : bool
        If the csv file has a header or not
    encoding : str (default utf-8)
        File encoding
    kwargs : other...
        Various choices about dialect
    """
    canonical_extension = 'csv'

    def __init__(self, path, has_header=None, encoding='utf-8',
                 sniff_nbytes=10000, **kwargs):
        self.path = path
        self._has_header = has_header
        self.encoding = encoding or 'utf-8'
        self._kwargs = kwargs
        self._sniff_nbytes = sniff_nbytes

    def _sniff_dialect(self, path):
        kwargs = self._kwargs
        dialect = sniff_dialect(path, self._sniff_nbytes,
                                encoding=self.encoding)
        kwargs = merge(dialect, keymap(alias, kwargs))
        return valfilter(lambda x: x is not None,
                         dict((d, kwargs[d])
                              for d in dialect_terms if d in kwargs))

    @property
    def dialect(self):
        with sample(self) as fn:
            return self._sniff_dialect(fn)

    @property
    def has_header(self):
        if self._has_header is None:
            with sample(self) as fn:
                with open(fn, mode='rb') as f:
                    raw = f.read()
                self._has_header = not raw or infer_header(fn,
                                                           self._sniff_nbytes,
                                                           encoding=self.encoding)
        return self._has_header


@sample.register(CSV)
@contextmanager
def sample_csv(csv, length=8192, **kwargs):
    should_delete = not os.path.exists(csv.path)
    if should_delete:
        with open_file(csv.path, mode='wb'):
            pass
    try:
        with open_file(csv.path, mode='rb') as f:
            with tmpfile(csv.canonical_extension) as fn:
                with open(fn, mode='wb') as tmpf:
                    tmpf.write(f.read(length))
                yield fn
    finally:
        if should_delete:
            os.remove(csv.path)


@append.register(CSV, object)
def append_object_to_csv(c, seq, **kwargs):
    append(c, convert(chunks(pd.DataFrame), seq, **kwargs), **kwargs)
    return c


compressed_open = {'gz': gzip.open, 'bz2': bz2.BZ2File}


@append.register(CSV, pd.DataFrame)
def append_dataframe_to_csv(c, df, dshape=None, **kwargs):
    if not os.path.exists(c.path) or not os.path.getsize(c.path):
        has_header = kwargs.pop('header', c.has_header)
    else:
        has_header = False
    sep = kwargs.get('sep',
                     kwargs.get('delimiter', c.dialect.get('delimiter', ',')))
    encoding = kwargs.get('encoding', c.encoding)

    if ext(c.path) in compressed_open:
        if sys.version_info[0] >= 3:
            kwargs['mode'] = 'at'
            kwargs['encoding'] = encoding
        elif sys.version_info[0] == 2:
            kwargs['mode'] = 'ab' if sys.platform == 'win32' else 'at'
        f = compressed_open[ext(c.path)](c.path, **kwargs)
    else:
        f = c.path

    try:
        df.to_csv(f, mode='a', header=has_header, index=False, sep=sep,
                  encoding=encoding)
    finally:
        if hasattr(f, 'close'):
            f.close()
    return c


@append.register(CSV, chunks(pd.DataFrame))
def append_iterator_to_csv(c, cs, **kwargs):
    for chunk in cs:
        append(c, chunk, **kwargs)
    return c


@convert.register(pd.DataFrame, (Temp(CSV), CSV), cost=20.0)
def csv_to_dataframe(c, dshape=None, chunksize=None, nrows=None, **kwargs):
    try:
        return _csv_to_dataframe(c, dshape=dshape, chunksize=chunksize,
                                 nrows=nrows, **kwargs)
    except StopIteration:
        if nrows:
            return _csv_to_dataframe(c, dshape=dshape, chunksize=chunksize,
                                     **kwargs)
        else:
            raise


def _csv_to_dataframe(c, dshape=None, chunksize=None, **kwargs):
    header = {False: None, True: 0}.get(
        kwargs.pop('has_header', c.has_header), 'infer')

    sep = kwargs.pop(
        'sep', kwargs.pop('delimiter', c.dialect.get('delimiter', ',')))
    encoding = kwargs.pop('encoding', c.encoding)

    if dshape:
        dtypes, parse_dates = dshape_to_pandas(dshape)
        if isrecord(dshape.measure):
            names = kwargs.get('names', dshape.measure.names)
        else:
            names = kwargs.get('names')
    else:
        dtypes = parse_dates = names = None

    usecols = kwargs.pop('usecols', None)
    if parse_dates and usecols:
        parse_dates = [col for col in parse_dates if col in usecols]

    compression = kwargs.pop('compression',
                             {'gz': 'gzip', 'bz2': 'bz2'}.get(ext(c.path)))

    # See read_csv docs for header for reasoning
    if names:
        try:
            found_names = pd.read_csv(c.path, encoding=encoding,
                                      compression=compression, nrows=1)
        except StopIteration:
            found_names = pd.read_csv(c.path, encoding=encoding,
                                      compression=compression)
    if names and header == 'infer':
        if [n.strip() for n in found_names] == [n.strip() for n in names]:
            header = 0
        elif (all(re.match('^\s*\D\w*\s*$', n) for n in found_names) and
                not all(dt == datashape.string for dt in dshape.measure.types)):
            header = 0
        else:
            header = None

    kwargs = keyfilter(keywords(pd.read_csv).__contains__, kwargs)
    return pd.read_csv(c.path,
                       header=header,
                       sep=sep,
                       encoding=encoding,
                       dtype=dtypes,
                       parse_dates=parse_dates,
                       names=names,
                       compression=compression,
                       chunksize=chunksize,
                       usecols=usecols,
                       **kwargs)


@convert.register(chunks(pd.DataFrame), (Temp(CSV), CSV), cost=10.0)
def CSV_to_chunks_of_dataframes(c, chunksize=2 ** 20, **kwargs):
    # Load a small 1000 line DF to start
    # This helps with rapid viewing of a large CSV file
    first = csv_to_dataframe(c, nrows=1000, **kwargs)
    if len(first) == 1000:
        rest = csv_to_dataframe(
            c, chunksize=chunksize, skiprows=1000, **kwargs)
    else:
        rest = []

    def _():
        yield first
        for df in rest:
            yield df
    return chunks(pd.DataFrame)(_)


@discover.register(CSV)
def discover_csv(c, nrows=1000, **kwargs):
    df = csv_to_dataframe(c, nrows=nrows, **kwargs)
    df = coerce_datetimes(df)

    columns = [str(c) if not isinstance(c, (str, unicode)) else c
               for c in df.columns]
    df.columns = [c.strip() for c in columns]

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


@resource.register('.+\.(csv|tsv|ssv|data|dat)(\.gz|\.bz2?)?')
def resource_csv(uri, **kwargs):
    return CSV(uri, **kwargs)


@resource.register('.*\*.+', priority=12)
def resource_glob(uri, **kwargs):
    filenames = sorted(glob(uri))
    r = resource(filenames[0], **kwargs)
    return chunks(type(r))([resource(u, **kwargs) for u in sorted(glob(uri))])


@convert.register(chunks(pd.DataFrame), (chunks(CSV), chunks(Temp(CSV))),
                  cost=10.0)
def convert_glob_of_csvs_to_chunks_of_dataframes(csvs, **kwargs):
    def _():
        return concat(convert(chunks(pd.DataFrame), csv, **kwargs)
                      for csv in csvs)
    return chunks(pd.DataFrame)(_)


@convert.register(Temp(CSV), (pd.DataFrame, chunks(pd.DataFrame)))
def convert_dataframes_to_temporary_csv(data, **kwargs):
    fn = '.%s.csv' % uuid.uuid1()
    csv = Temp(CSV)(fn, **kwargs)
    return append(csv, data, **kwargs)


@dispatch(CSV)
def drop(c):
    os.unlink(c.path)


ooc_types.add(CSV)
