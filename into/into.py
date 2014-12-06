import numpy as np
import pandas as pd
from collections import Iterator
from .core import NetworkDispatcher

into = NetworkDispatcher('into')

@into.register(np.recarray, pd.DataFrame, cost=1.0)
def into_recarray(x, df, **kwargs):
    return df.to_records(index=False)


@into.register(pd.DataFrame, np.recarray, cost=1.0)
def recarray_to_dataframe(x, df, **kwargs):
    return pd.DataFrame(df)


@into.register(pd.Series, list, cost=1.0)
def recarray_to_dataframe(x, L, **kwargs):
    return pd.Series(L)


@into.register(list, pd.Series, cost=1.0)
def series_to_list(_, s, **kwargs):
    return s.tolist()


@into.register(list, np.ndarray, cost=1.0)
def numpy_to_list(_, x, **kwargs):
    return x.tolist()


@into.register(set, (list, tuple, set), cost=1.0)
def iterable_to_set(_, x, **kwargs):
    return set(x)


@into.register(list, (list, tuple, set), cost=1.0)
def iterable_to_list(_, x, **kwargs):
    return list(x)


@into.register(tuple, (list, tuple, set), cost=1.0)
def iterable_to_tuple(_, x, **kwargs):
    return tuple(x)


@into.register(np.recarray, list, cost=1.0)
def list_to_recarray(_, seq, dshape=None, **kwargs):
    dtype = to_numpy_dtype(dshape)
    return np.array(seq, dtype=dtype)


@into.register(Iterator, list, cost=1.0)
def list_to_iterator(_, L, cost=1.0):
    return iter(L)


@into.register(list, Iterator, cost=1.0)
def iterator_to_list(_, seq, cost=1.0):
    return list(seq)
