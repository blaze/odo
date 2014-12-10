from __future__ import absolute_import, division, print_function

import csv

import datashape
from datashape import discover
from datashape.predicates import isrecord
from collections import Iterator
from toolz import concat
import pandas
import pandas as pd
import os
import gzip
import bz2

from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..numpy_dtype import dshape_to_pandas
from .pandas import coerce_datetimes

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
    def __init__(self, path, has_header=None, encoding='utf-8', **kwargs):
        self.path = path
        self.has_header = has_header
        self.encoding = encoding
        self.dialect = {d: kwargs[d] for d in dialect_terms if d in kwargs}


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


def ext(path):
    _, e = os.path.splitext(path)
    return e.lstrip('.')


@convert.register(pd.DataFrame, CSV)
def csv_to_DataFrame(c, dshape=None, chunksize=None, **kwargs):
    has_header = kwargs.get('has_header', c.has_header)
    if has_header is False:
        header = None
    elif has_header is True:
        header = 0
    else:
        header = 'infer'
    sep = kwargs.get('sep', kwargs.get('delimiter', c.dialect.get('delimiter', ',')))
    encoding = kwargs.get('encoding', c.encoding)
    if dshape:
        dtypes, parse_dates = dshape_to_pandas(dshape)
        if isrecord(dshape.measure):
            names = kwargs.get('names', dshape.measure.names)
        else:
            names = kwargs.get('names')
    else:
        dtypes = parse_dates = names = None
    compression={'gz': 'gzip',
                 'bz2': 'bz2'}.get(ext(c.path))

    return pandas.read_csv(c.path,
                             header=header,
                             sep=sep,
                             encoding=encoding,
                             dtype=dtypes,
                             parse_dates=parse_dates,
                             names=names,
                             compression=compression,
                             chunksize=chunksize)


@convert.register(chunks(pd.DataFrame), CSV)
def CSV_to_chunks_of_dataframes(c, chunksize=2**20, **kwargs):
    return chunks(pd.DataFrame)(
            lambda: iter(csv_to_DataFrame(c, chunksize=chunksize, **kwargs)))


@discover.register(CSV)
def discover_csv(c):
    df = csv_to_DataFrame(c, chunksize=50).get_chunk()
    df = coerce_datetimes(df)

    return datashape.var * discover(df).subshape[0]


@resource.register('.+\.(csv|tsv|ssv|data|dat)(\.gz|\.bz)?')
def resource_csv(uri, **kwargs):
    return CSV(uri, **kwargs)


from glob import glob
@resource.register('.+\*.+')
def resource_glob(uri, **kwargs):
    filenames = sorted(glob(uri))
    r = resource(filenames[0], **kwargs)
    return chunks(type(r))([resource(u, **kwargs) for u in sorted(glob(uri))])

    # Alternatively check each time we iterate?
    def _():
        return (resource(u, **kwargs) for u in glob(uri))
    return chunks(type(r))(_)


@convert.register(chunks(pd.DataFrame), chunks(CSV))
def convert_glob_of_csvs_to_chunks_of_dataframes(csvs, **kwargs):
    def _():
        return concat(convert(chunks(pd.DataFrame), csv, **kwargs) for csv in csvs)
    return chunks(pd.DataFrame)(_)



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
