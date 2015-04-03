from __future__ import absolute_import, division, print_function
from operator import add
from collections import Iterator

import numpy as np
from toolz import merge, accumulate
from datashape.dispatch import dispatch
from datashape import DataShape

from dask.array.core import rec_concatenate, Array, getem, get, names, from_array
from dask.bag.core import Bag
from dask.core import flatten
from dask.compatibility import long

from odo import append, chunks, convert, discover, into, TextFile
from ..utils import keywords

##############
# dask.Array #
##############

@discover.register(Array)
def discover_dask_array(a, **kwargs):
    block = a._get_block(*([0] * a.ndim))
    return DataShape(*(a.shape + (discover(block).measure,)))


arrays = [np.ndarray]
try:
    import h5py
    arrays.append(h5py.Dataset)

    @dispatch(h5py.Dataset, (int, long))
    def resize(x, size):
        s = list(x.shape)
        s[0] = size
        return resize(x, tuple(s))

    @dispatch(h5py.Dataset, tuple)
    def resize(x, shape):
        return x.resize(shape)
except ImportError:
    pass
try:
    import bcolz
    arrays.append(bcolz.carray)

    @dispatch(bcolz.carray, (int, long))
    def resize(x, size):
        return x.resize(size)
except ImportError:
    pass


@convert.register(Array, tuple(arrays), cost=1.)
def array_to_dask(x, name=None, blockshape=None, **kwargs):
    if blockshape is None:
        raise NotImplementedError("blockshape cannot be None")
    return from_array(x, blockshape=blockshape, name=name, **kwargs)


@convert.register(np.ndarray, Array, cost=10.)
def dask_to_numpy(x, **kwargs):
    return rec_concatenate(get(x.dask, x._keys(), **kwargs))


@convert.register(float, Array, cost=10.)
def dask_to_float(x, **kwargs):
    return x.compute()


@append.register(tuple(arrays), Array)
def store_Array_in_ooc_data(out, arr, inplace=False, **kwargs):
    if not inplace:
        # Resize output dataset to accept new data
        assert out.shape[1:] == arr.shape[1:]
        resize(out, out.shape[0] + arr.shape[0])  # elongate
    return arr.store(out)

############
# dask.bag #
############

@convert.register(Iterator, Bag)
def bag_to_iterator(x, **kwargs):
    return iter(x)


@convert.register(Bag, chunks(TextFile))
def bag_to_iterator(x, **kwargs):
    return Bag.from_filenames([tf.path for tf in x])


@convert.register(Bag, list)
def bag_to_iterator(x, **kwargs):
    keys = keywords(Bag.from_sequence)
    kwargs2 = dict((k, v) for k, v in kwargs.items() if k in keys)
    return Bag.from_sequence(x, **kwargs2)
