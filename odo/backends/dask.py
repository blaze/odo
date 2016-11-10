from __future__ import absolute_import, division, print_function
from collections import Iterator

import numpy as np
import pandas as pd
from datashape.dispatch import dispatch
from datashape import from_numpy, var

from dask.array.core import Array, from_array
from dask.bag.core import Bag
import dask.bag as db
from dask.compatibility import long
import dask.dataframe as dd

from odo import append, chunks, convert, discover, TextFile
from ..utils import filter_kwargs


@discover.register(Array)
def discover_dask_array(a, **kwargs):
    return from_numpy(a.shape, a.dtype)


@discover.register(dd.Series)
@discover.register(dd.DataFrame)
def discover_dask_dataframe(df):
    return var * discover(df.head()).measure


arrays = [np.ndarray]

try:
    import h5py
except ImportError:
    pass
else:
    arrays.append(h5py.Dataset)

    @dispatch(h5py.Dataset, (int, long))
    def resize(x, size):
        s = list(x.shape)
        s[0] = size
        return resize(x, tuple(s))

    @dispatch(h5py.Dataset, tuple)
    def resize(x, shape):
        return x.resize(shape)

try:
    import bcolz
except ImportError:
    pass
else:
    arrays.append(bcolz.carray)

    @dispatch(bcolz.carray, (int, long))
    def resize(x, size):
        return x.resize(size)


@convert.register(Array, tuple(arrays), cost=1.)
def array_to_dask(x, name=None, chunks=None, **kwargs):
    if chunks is None:
        raise ValueError("chunks cannot be None")
    return from_array(x, chunks=chunks, name=name,
                      **filter_kwargs(from_array, kwargs))


@convert.register(np.ndarray, Array, cost=10.)
def dask_to_numpy(x, **kwargs):
    return np.array(x)


@convert.register(pd.DataFrame, dd.DataFrame, cost=200)
@convert.register(pd.Series, dd.Series, cost=200)
@convert.register(float, Array, cost=200)
def dask_to_other(x, **kwargs):
    return x.compute()


@append.register(tuple(arrays), Array)
def store_Array_in_ooc_data(out, arr, inplace=False, **kwargs):
    if not inplace:
        # Resize output dataset to accept new data
        assert out.shape[1:] == arr.shape[1:]
        resize(out, out.shape[0] + arr.shape[0])  # elongate
    arr.store(out)
    return out


@convert.register(Iterator, Bag)
def bag_to_iterator(x, **kwargs):
    return iter(x)


@convert.register(Bag, chunks(TextFile))
def bag_to_iterator(x, **kwargs):
    return db.read_text([tf.path for tf in x])


@convert.register(Bag, list)
def bag_to_iterator(x, **kwargs):
    return db.from_sequence(x, **filter_kwargs(db.from_sequence, kwargs))


@convert.register(dd.DataFrame, pd.DataFrame, cost=1.)
def pandas_dataframe_to_dask_dataframe(x, npartitions=None, **kwargs):
    if npartitions is None:
        raise ValueError("npartitions cannot be None")
    return dd.from_pandas(x, npartitions=npartitions,
                          **filter_kwargs(dd.from_pandas, kwargs))
