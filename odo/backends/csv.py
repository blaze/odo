from __future__ import absolute_import, division, print_function

import re
import datashape
from datashape import discover, Record, Option
from datashape.predicates import isrecord
from datashape.dispatch import dispatch
from toolz import concat, keyfilter, keymap
import pandas
import pandas as pd
import os
import gzip
import bz2
import uuid

from ..compatibility import unicode
from ..utils import keywords, ext
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

def alias(key):
    """ Alias kwarg dialect keys to normalized set

    >>> alias('sep')
    'delimiter'
    """
    key = key.lower()
    return aliases.get(key, key)


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

    def __init__(self, path, has_header='no-input', encoding='utf-8', **kwargs):
        self.path = path
        if has_header == 'no-input':
            if not os.path.exists(path):
                self.has_header = True
            else:
                self.has_header = None
        else:
            self.has_header = has_header
        self.encoding = encoding
        kwargs = keymap(alias, kwargs)
        self.dialect = dict((d, kwargs[d]) for d in dialect_terms
                                           if d in kwargs)


@append.register(CSV, object)
def append_object_to_csv(c, seq, **kwargs):
    append(c, convert(chunks(pd.DataFrame), seq, **kwargs), **kwargs)
    return c


compressed_open = {'gz': gzip.open, 'bz2': bz2.BZ2File}

@append.register(CSV, pd.DataFrame)
def append_dataframe_to_csv(c, df, dshape=None, **kwargs):
    if not os.path.exists(c.path) or os.path.getsize(c.path) == 0:
        has_header = kwargs.pop('has_header', c.has_header)
    else:
        has_header = False
    if has_header is None:
        has_header = True

    sep = kwargs.get('sep', kwargs.get('delimiter', c.dialect.get('delimiter', ',')))
    encoding=kwargs.get('encoding', c.encoding)

    if ext(c.path) in compressed_open:
        f = compressed_open[ext(c.path)](c.path, mode='a')
    else:
        f = c.path

    df.to_csv(f, mode='a',
                    header=has_header,
                    index=False,
                    sep=sep,
                    encoding=encoding)
    if hasattr(f, 'flush'):
        f.flush()
        f.close()

    return c


@append.register(CSV, chunks(pd.DataFrame))
def append_iterator_to_csv(c, cs, **kwargs):
    for chunk in cs:
        append(c, chunk, **kwargs)
    return c


@convert.register(pd.DataFrame, (Temp(CSV), CSV), cost=20.0)
def csv_to_DataFrame(c, dshape=None, chunksize=None, nrows=None, **kwargs):
    try:
        return _csv_to_DataFrame(c, dshape=dshape,
                chunksize=chunksize, nrows=nrows, **kwargs)
    except StopIteration:
        if nrows:
            return _csv_to_DataFrame(c, dshape=dshape, chunksize=chunksize, **kwargs)
        else:
            raise

def _csv_to_DataFrame(c, dshape=None, chunksize=None, **kwargs):
    has_header = kwargs.pop('has_header', c.has_header)
    if has_header is False:
        header = None
    elif has_header is True:
        header = 0
    else:
        header = 'infer'

    sep = kwargs.pop('sep', kwargs.pop('delimiter', c.dialect.get('delimiter', ',')))
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

    kwargs2 = keyfilter(keywords(pandas.read_csv).__contains__, kwargs)
    return pandas.read_csv(c.path,
                             header=header,
                             sep=sep,
                             encoding=encoding,
                             dtype=dtypes,
                             parse_dates=parse_dates,
                             names=names,
                             compression=compression,
                             chunksize=chunksize,
                             usecols=usecols,
                             **kwargs2)


@convert.register(chunks(pd.DataFrame), (Temp(CSV), CSV), cost=10.0)
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
        df = csv_to_DataFrame(c, chunksize=50, has_header=False).get_chunk()
        df = coerce_datetimes(df)

    columns = [str(c) if not isinstance(c, (str, unicode)) else c
                for c in df.columns]
    df.columns = [c.strip() for c in columns]

    # Replace np.nan with None.  Forces type string rather than flaot
    for col in df.columns:
        if df[col].count() == 0:
            df[col] = [None] * len(df)

    measure = discover(df).measure

    # Use Series.notnull to determine Option-ness
    measure2 = Record([[name, Option(typ)
                              if (~df[name].notnull()).any()
                              and not isinstance(typ, Option) else typ]
                      for name, typ in zip(measure.names, measure.types)])

    return datashape.var * measure2


@resource.register('.+\.(csv|tsv|ssv|data|dat)(\.gz|\.bz)?')
def resource_csv(uri, **kwargs):
    return CSV(uri, **kwargs)


from glob import glob
@resource.register('.*\*.+', priority=12)
def resource_glob(uri, **kwargs):
    filenames = sorted(glob(uri))
    r = resource(filenames[0], **kwargs)
    return chunks(type(r))([resource(u, **kwargs) for u in sorted(glob(uri))])

    # Alternatively check each time we iterate?
    def _():
        return (resource(u, **kwargs) for u in glob(uri))
    return chunks(type(r))(_)


@convert.register(chunks(pd.DataFrame), (chunks(CSV), chunks(Temp(CSV))), cost=10.0)
def convert_glob_of_csvs_to_chunks_of_dataframes(csvs, **kwargs):
    def _():
        return concat(convert(chunks(pd.DataFrame), csv, **kwargs) for csv in csvs)
    return chunks(pd.DataFrame)(_)


@convert.register(Temp(CSV), (pd.DataFrame, chunks(pd.DataFrame)))
def convert_dataframes_to_temporary_csv(data, **kwargs):
    fn = '.%s.csv' % uuid.uuid1()
    csv = Temp(CSV)(fn, **kwargs)
    return append(csv, data, **kwargs)


@dispatch(CSV)
def drop(c):
    os.unlink(c.path)


def infer_header(csv, encoding='utf-8', **kwargs):
    """ Guess if csv file has a header or not

    This uses Pandas to read a sample of the file, then looks at the column
    names to see if they are all word-like.

    Returns True or False
    """
    compression = kwargs.pop('compression',
            {'gz': 'gzip', 'bz2': 'bz2'}.get(ext(csv.path)))
    # See read_csv docs for header for reasoning
    try:
        df = pd.read_csv(csv.path, encoding=encoding,
                         compression=compression, nrows=5)
    except StopIteration:
        df = pd.read_csv(csv.path, encoding=encoding,
                                  compression=compression)
    return (len(df) > 0 and
            all(re.match('^\s*\D\w*\s*$', n) for n in df.columns) and
            not all(dt == 'O' for dt in df.dtypes))


ooc_types.add(CSV)
