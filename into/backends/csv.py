from __future__ import absolute_import, division, print_function

import re
import sys
import os
import gzip
import bz2
import codecs
from contextlib import contextmanager, closing
from glob import glob

import datashape
from datashape import discover, Record, Option
from datashape.predicates import isrecord
from datashape.dispatch import dispatch
from toolz import concat, keyfilter
import pandas
import pandas as pd

from ..utils import keywords
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..numpy_dtype import dshape_to_pandas
from .pandas import coerce_datetimes


PY = sys.version_info[:2]


BOMS = {
    'UTF-8': codecs.BOM_UTF8,
    'UTF-16LE': codecs.BOM_UTF16_LE,
    'UTF-16BE': codecs.BOM_UTF16_BE,
    'UTF-32LE': codecs.BOM_UTF32_LE,
    'UTF-32BE': codecs.BOM_UTF32_BE,
}


dialect_terms = '''delimiter doublequote escapechar lineterminator quotechar
quoting skipinitialspace strict'''.split()


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
    def __init__(self, path, has_header='no-input', encoding=None, **kwargs):
        self.path = path
        if has_header == 'no-input':
            if not os.path.exists(path):
                self.has_header = True
            else:
                self.has_header = None
        else:
            self.has_header = has_header
        self.encoding = encoding
        self.dialect = dict((d, kwargs[d]) for d in dialect_terms
                                           if d in kwargs)


@contextmanager
def gzip_open(*args, **kwargs):
    with closing(gzip.open(*args, **kwargs)) as f:
        yield f


@contextmanager
def bz2_open(*args, **kwargs):
    with closing(bz2.BZ2File(*args, **kwargs)) as f:
        yield f


compressed_open = {'gz': gzip_open, 'bz2': bz2_open, 'gzip': gzip_open}


def get_bom_index(filename, bom):
    if not os.path.exists(filename):
        return -1
    with open(filename, mode='rb') as f:
        if bom is not None:
            return f.read(32).find(bom)
        return -1


@contextmanager
def csvopen(filename, encoding=None, compression=None, mode='rb'):
    if compression is not None and encoding is not None:
        raise ValueError('Cannot pass both encoding and compression arguments')
    if compression is not None and compression in compressed_open:
        with compressed_open[compression](filename, mode=mode) as f:
            yield f
    else:
        bom = BOMS.get(encoding)
        index = get_bom_index(filename, bom=bom)
        skipbytes = 0 if index == -1 else index + len(bom)

        with codecs.open(filename, mode=mode, encoding=encoding) as f:
            if skipbytes:
                f.seek(skipbytes)
            yield f


def ext(path):
    _, e = os.path.splitext(path)
    return e.lstrip('.')


@append.register(CSV, object)
def append_object_to_csv(c, seq, **kwargs):
    append(c, convert(chunks(pd.DataFrame), seq, **kwargs), **kwargs)
    return c


@append.register(CSV, pd.DataFrame)
def append_dataframe_to_csv(c, df, dshape=None, **kwargs):
    if not os.path.exists(c.path) or os.path.getsize(c.path) == 0:
        has_header = kwargs.pop('has_header', c.has_header)
    else:
        has_header = False
    if has_header is None:
        has_header = True

    sep = kwargs.get('sep', kwargs.get('delimiter',
                                       c.dialect.get('delimiter', ',')))
    encoding = kwargs.get('encoding', c.encoding)

    with csvopen(c.path, mode='ab' if PY[0] == 2 else 'a', encoding=encoding,
                 compression=ext(c.path)) as f:
        df.to_csv(f, header=has_header, index=False, sep=sep)
    return c


@append.register(CSV, chunks(pd.DataFrame))
def append_iterator_to_csv(c, cs, **kwargs):
    for chunk in cs:
        append(c, chunk, **kwargs)
    return c


@convert.register(pd.DataFrame, CSV, cost=20.0)
def csv_to_DataFrame(c, dshape=None, chunksize=None, nrows=None, **kwargs):
    try:
        return _csv_to_DataFrame(c, dshape=dshape, chunksize=chunksize,
                                 nrows=nrows, **kwargs)
    except StopIteration:
        if nrows:
            return _csv_to_DataFrame(c, dshape=dshape, chunksize=chunksize,
                                     **kwargs)
        raise


def _csv_to_DataFrame(c, dshape=None, chunksize=None, **kwargs):
    has_header = kwargs.pop('has_header', c.has_header)
    if has_header is False:
        header = None
    elif has_header is True:
        header = 0
    else:
        header = 'infer'

    sep = kwargs.pop('sep', kwargs.pop('delimiter',
                                       c.dialect.get('delimiter', ',')))
    encoding = kwargs.get('encoding', c.encoding)

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
            with csvopen(c.path, encoding=encoding,
                                  compression=compression) as f:
                found_names = pd.read_csv(f, nrows=1)
        except StopIteration:
            with csvopen(c.path, encoding=encoding,
                                  compression=compression) as f:
                found_names = pd.read_csv(f)

        if [n.strip() for n in found_names] == [n.strip() for n in names]:
            header = 0
        elif (all(re.match('^\s*\D\w*\s*$', n) for n in found_names) and
                not all(dt == datashape.string for dt in dshape.measure.types)):
            header = 0
        else:
            header = None

    kwargs2 = keyfilter(keywords(pandas.read_csv).__contains__, kwargs)
    reader = dense_reader if chunksize is None else yield_reader

    return reader(c.path,
                  header=header,
                  encoding=encoding,
                  compression=compression,
                  sep=sep,
                  dtype=dtypes,
                  parse_dates=parse_dates,
                  names=names,
                  chunksize=chunksize,
                  usecols=usecols,
                  **kwargs2)


def yield_reader(filename, encoding=None, compression=None, *args, **kwargs):
    with csvopen(filename, encoding=encoding,
                          compression=compression) as f:
        for chunk in pandas.read_csv(f, *args, **kwargs):
            yield chunk


def dense_reader(filename, encoding=None, compression=None, *args, **kwargs):
    with csvopen(filename, encoding=encoding,
                          compression=compression) as f:
        return pd.read_csv(f, *args, **kwargs)


@convert.register(chunks(pd.DataFrame), CSV, cost=10.0)
def CSV_to_chunks_of_dataframes(c, chunksize=2**20, **kwargs):
    # Load a small 1000 line DF to start
    # This helps with rapid viewing of a large CSV file
    first = csv_to_DataFrame(c, nrows=1000, **kwargs)
    if len(first) == 1000:
        rest = csv_to_DataFrame(c, chunksize=chunksize, skiprows=1000, **kwargs)
    else:
        rest = []
    def _():
        yield first
        for df in rest:
            yield df
    return chunks(pd.DataFrame)(_)


@discover.register(CSV)
def discover_csv(c, nrows=1000, **kwargs):
    df = csv_to_DataFrame(c, nrows=nrows, **kwargs)
    df = coerce_datetimes(df)

    if (not list(df.columns) == list(range(len(df.columns)))
        and any(re.match('^[-\d_]*$', c) for c in df.columns)):
        df = next(csv_to_DataFrame(c, chunksize=50, has_header=False))
        df = coerce_datetimes(df)

    df.columns = [str(c).strip() for c in df.columns]

    # Replace np.nan with None.  Forces type string rather than flaot
    for col in df.columns:
        if df[col].count() == 0:
            df[col] = [None] * len(df)

    measure = discover(df).measure

    # Use Series.notnull to determine Option-ness
    measure2 = Record([[name, Option(typ) if (~df[name].notnull()).any() else typ]
                      for name, typ in zip(measure.names, measure.types)])

    return datashape.var * measure2


@resource.register('.+\.(csv|tsv|ssv|data|dat)(\.gz|\.bz)?')
def resource_csv(uri, **kwargs):
    return CSV(uri, **kwargs)


@resource.register('.+\*.+', priority=12)
def resource_glob(uri, **kwargs):
    filenames = sorted(glob(uri))
    r = resource(filenames[0], **kwargs)
    return chunks(type(r))([resource(u, **kwargs) for u in sorted(glob(uri))])

    # Alternatively check each time we iterate?
    def _():
        return (resource(u, **kwargs) for u in glob(uri))
    return chunks(type(r))(_)


@convert.register(chunks(pd.DataFrame), chunks(CSV), cost=10.0)
def convert_glob_of_csvs_to_chunks_of_dataframes(csvs, **kwargs):
    def _():
        return concat(convert(chunks(pd.DataFrame), csv, **kwargs) for csv in csvs)
    return chunks(pd.DataFrame)(_)


@dispatch(CSV)
def drop(c):
    os.unlink(c.path)


"""
@append.register(CSV, Iterator)
def append_iterator_to_csv(c, seq, dshape=None, **kwargs):
    f = open(c.path, mode='a')

    # TODO: handle dict case
    writer = csv.writer(f, **c.dialect)
    writer.writerows(seq)
    return c


try:
    from dynd import nd
    @convert.register(Iterator, CSV)
    def csv_to_iterator(c, chunksize=1024, dshape=None, **kwargs):
        f = open(c.path)
        reader = csv.reader(f, **c.dialect)
        if c.header:
            next(reader)

        # Launder types through DyND
        seq = pipe(reader, partition_all(chunksize),
                           map(partial(nd.array, type=str(dshape.measure))),
                           map(partial(nd.as_py, tuple=True)),
                           concat)

        return reader
except ImportError:
    pass
"""


ooc_types.add(CSV)
