import numpy as np
import pandas as pd
from collections import Iterator
import datashape
from .core import NetworkDispatcher

convert = NetworkDispatcher('convert')

@convert.register(np.recarray, pd.DataFrame, cost=1.0)
def convert_recarray(df, **kwargs):
    return df.to_records(index=False)


@convert.register(pd.DataFrame, np.recarray, cost=1.0)
def recarray_to_dataframe(df, **kwargs):
    return pd.DataFrame(df)


@convert.register(pd.Series, np.ndarray, cost=1.0)
def recarray_to_dataframe(x, **kwargs):
    return pd.Series(x)


@convert.register(np.ndarray, pd.Series, cost=1.0)
def series_to_array(s, **kwargs):
    return np.array(s)


@convert.register(list, np.ndarray, cost=1.0)
def numpy_to_list(x, **kwargs):
    return x.tolist()


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
