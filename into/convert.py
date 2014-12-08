from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd
from toolz import concat
from collections import Iterator
import datashape
from .core import NetworkDispatcher
from .chunks import chunks

convert = NetworkDispatcher('convert')


def identity(x, **kwargs):
    return x

@convert.register(np.recarray, pd.DataFrame, cost=1.0)
def convert_recarray(df, **kwargs):
    return df.to_records(index=False)


@convert.register(pd.DataFrame, np.recarray, cost=1.0)
def recarray_to_dataframe(df, **kwargs):
    return pd.DataFrame(df)


@convert.register(pd.Series, np.ndarray, cost=1.0)
def recarray_to_dataframe(x, **kwargs):
    return pd.Series(x)


@convert.register(pd.Series, pd.DataFrame, cost=1.0)
def DataFrame_to_Series(x, **kwargs):
    assert len(x.columns) == 1
    return x[x.columns[0]]


@convert.register(pd.DataFrame, pd.Series, cost=1.0)
def series_to_dataframe(x, **kwargs):
    return x.to_frame()

convert.register(np.recarray, np.ndarray, cost=0.0)(identity)
convert.register(np.ndarray, np.recarray, cost=0.0)(identity)


@convert.register(np.ndarray, pd.Series, cost=1.0)
def series_to_array(s, **kwargs):
    return np.array(s)


@convert.register(list, np.ndarray, cost=1.0)
def numpy_to_list(x, **kwargs):
    return x.tolist()


@convert.register(np.ndarray, chunks(np.ndarray), cost=1.0)
def numpy_to_list(c, **kwargs):
    return np.concatenate(list(c))


@convert.register(set, (list, tuple), cost=1.0)
def iterable_to_set(x, **kwargs):
    return set(x)


@convert.register(list, (tuple, set), cost=1.0)
def iterable_to_list(x, **kwargs):
    return list(x)


@convert.register(tuple, (list, set), cost=1.0)
def iterable_to_tuple(x, **kwargs):
    return tuple(x)


@convert.register(np.ndarray, list, cost=1.0)
def list_to_recarray(seq, dshape=None, **kwargs):
    dtype = datashape.to_numpy_dtype(dshape)
    return np.array(seq, dtype=dtype)


@convert.register(Iterator, list, cost=1.0)
def list_to_iterator(L, **kwargs):
    return iter(L)


@convert.register(list, Iterator, cost=1.0)
def iterator_to_list(seq, **kwargs):
    return list(seq)


@convert.register(Iterator, chunks(np.ndarray))
def numpy_chunks_to_iterator(c, **kwargs):
    return concat(map(into(Iterator), c))


@convert.register(chunks(np.ndarray), Iterator)
def iterator_to_numpy_chunks(seq, chunksize=1024, **kwargs):
    from toolz.curried import pipe, partition_all, map
    return pipe(seq, parition_all(chunksize),
                     map(into(np.ndarray)),
                     chunks(np.ndarray))
