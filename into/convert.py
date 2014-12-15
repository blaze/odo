from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd
from datashape.predicates import isscalar
from toolz import concat, curry, partition_all
from collections import Iterator, Iterable
import datashape
from .core import NetworkDispatcher, ooc_types
from .chunks import chunks, Chunks
from .numpy_dtype import dshape_to_numpy


convert = NetworkDispatcher('convert')


def identity(x, **kwargs):
    """

    >>> identity('anything')
    'anything'
    """
    return x

@convert.register(np.ndarray, pd.DataFrame, cost=0.2)
def dataframe_to_numpy(df, dshape=None, **kwargs):
    dtype = dshape_to_numpy(dshape)
    x = df.to_records(index=False)
    if x.dtype != dtype:
        x = x.astype(dtype)
    return x


@convert.register(pd.DataFrame, np.ndarray, cost=1.0)
def numpy_to_dataframe(x, **kwargs):
    return pd.DataFrame(x)


@convert.register(pd.Series, np.ndarray, cost=1.0)
def numpy_to_series(x, **kwargs):
    return pd.Series(x)


@convert.register(pd.Series, pd.DataFrame, cost=0.1)
def DataFrame_to_Series(x, **kwargs):
    assert len(x.columns) == 1
    return x[x.columns[0]]


@convert.register(pd.DataFrame, pd.Series, cost=0.1)
def series_to_dataframe(x, **kwargs):
    return x.to_frame()

convert.register(np.recarray, np.ndarray, cost=0.0)(identity)
convert.register(np.ndarray, np.recarray, cost=0.0)(identity)


@convert.register(np.ndarray, pd.Series, cost=0.1)
def series_to_array(s, **kwargs):
    return np.array(s)


@convert.register(list, np.ndarray, cost=10.0)
def numpy_to_list(x, **kwargs):
    dt = None
    if x.dtype == 'M8[ns]':
        dt = 'M8[us]' # lose precision when going to Python datetime
    if x.dtype.fields and any(x.dtype[n] == 'M8[ns]' for n in x.dtype.names):
        dt = [(n, 'M8[us]' if x.dtype[n] == 'M8[ns]' else x.dtype[n])
                for n in x.dtype.names]
    if dt:
        return x.astype(dt).tolist()
    else:
        return x.tolist()


@convert.register(np.ndarray, chunks(np.ndarray), cost=1.0)
def numpy_chunks_to_numpy(c, **kwargs):
    return np.concatenate(list(c))


def ishashable(x):
    try:
        hash(x)
        return True
    except:
        return False


@convert.register(set, (list, tuple), cost=5.0)
def iterable_to_set(x, **kwargs):
    if x and isinstance(x[0], Iterable) and not ishashable(x):
        x = map(tuple, x)
    return set(x)


@convert.register(list, (tuple, set), cost=1.0)
def iterable_to_list(x, **kwargs):
    return list(x)


@convert.register(tuple, (list, set), cost=1.0)
def iterable_to_tuple(x, **kwargs):
    return tuple(x)


@convert.register(np.ndarray, list, cost=10.0)
def list_to_numpy(seq, dshape=None, **kwargs):
    if (seq and isinstance(seq[0], Iterable)
            and not ishashable(seq[0])
            and not isscalar(dshape)):
        seq = list(map(tuple, seq))
    dtype = dshape_to_numpy(dshape)
    return np.array(seq, dtype=dtype)


@convert.register(Iterator, list, cost=0.001)
def list_to_iterator(L, **kwargs):
    return iter(L)


@convert.register(list, Iterator, cost=1.0)
def iterator_to_list(seq, **kwargs):
    return list(seq)


@convert.register(Iterator, (chunks(pd.DataFrame), chunks(np.ndarray)), cost=10.0)
def numpy_chunks_to_iterator(c, **kwargs):
    return concat(convert(Iterator, chunk, **kwargs) for chunk in c)


@convert.register(chunks(np.ndarray), Iterator, cost=10.0)
def iterator_to_numpy_chunks(seq, chunksize=1024, **kwargs):
    seq2 = partition_all(chunksize, seq)
    first, rest = next(seq2), seq2
    x = convert(np.ndarray, first, **kwargs)
    def _():
        yield x
        for i in rest:
            yield convert(np.ndarray, i, **kwargs)
    return chunks(np.ndarray)(_)


@convert.register(chunks(pd.DataFrame), Iterator, cost=10.0)
def iterator_to_DataFrame_chunks(seq, chunksize=1024, **kwargs):
    seq2 = partition_all(chunksize, seq)
    first, rest = next(seq2), seq2
    df = convert(pd.DataFrame, first, **kwargs)
    def _():
        yield df
        for i in rest:
            yield convert(pd.DataFrame, i, **kwargs)
    return chunks(pd.DataFrame)(_)


@convert.register(tuple, np.record)
def numpy_record_to_tuple(rec, **kwargs):
    return rec.tolist()


@convert.register(chunks(np.ndarray), chunks(pd.DataFrame), cost=1.0)
def chunked_pandas_to_chunked_numpy(c, **kwargs):
    return chunks(np.ndarray)(lambda: (convert(np.ndarray, chunk, **kwargs) for chunk in c))

@convert.register(chunks(pd.DataFrame), chunks(np.ndarray), cost=1.0)
def chunked_numpy_to_chunked_pandas(c, **kwargs):
    return chunks(pd.DataFrame)(lambda: (convert(pd.DataFrame, chunk, **kwargs) for chunk in c))


@convert.register(chunks(np.ndarray), chunks(list), cost=10.0)
def chunked_list_to_chunked_numpy(c, **kwargs):
    return chunks(np.ndarray)(lambda: (convert(np.ndarray, chunk, **kwargs) for chunk in c))

@convert.register(chunks(list), chunks(np.ndarray), cost=10.0)
def chunked_numpy_to_chunked_list(c, **kwargs):
    return chunks(list)(lambda: (convert(list, chunk, **kwargs) for chunk in c))


ooc_types |= set([Iterator, Chunks])
