from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd

import datashape
from datashape import discover
from ..append import append
from ..convert import convert, ooc_types
from ..chunks import chunks
from ..resource import resource
from ..utils import filter_kwargs


@discover.register(pd.HDFStore)
def discover_hdfstore(f):
    d = dict()
    for key in f.keys():
        d2 = d
        key2 = key.lstrip('/')
        while '/' in key2:
            group, key2 = key2.split('/', 1)
            if group not in d2:
                d2[group] = dict()
            d2 = d2[group]
        d2[key2] = f.get_storer(key)
    return discover(d)


@discover.register(pd.io.pytables.Fixed)
def discover_hdfstore_storer(storer):
    f = storer.parent
    n = storer.shape
    if isinstance(n, list):
        n = n[0]
    measure = discover(f.select(storer.pathname, start=0, stop=10)).measure
    return n * measure


@convert.register(chunks(pd.DataFrame), pd.io.pytables.AppendableFrameTable)
def hdfstore_to_chunks_dataframes(data, chunksize=100000, **kwargs):
    if (isinstance(chunksize, (float, np.floating)) and
            not chunksize.is_integer()):
        raise TypeError('chunksize argument must be an integer, got %s' %
                        chunksize)

    chunksize = int(chunksize)

    def f():
        k = min(chunksize, 100)
        yield data.parent.select(data.pathname, start=0, stop=k)
        for chunk in data.parent.select(data.pathname, chunksize=chunksize,
                                        start=k):
            yield chunk
    return chunks(pd.DataFrame)(f)


@convert.register(pd.DataFrame, (pd.io.pytables.AppendableFrameTable,
                                 pd.io.pytables.FrameFixed))
def hdfstore_to_chunks_dataframes(data, **kwargs):
    return data.read()


pytables_h5py_explanation = """
You've run in to a conflict between the two HDF5 libraries in Python,
H5Py and PyTables.  You're trying to do something that requires PyTables but
H5Py was loaded first and the two libraries don't share well.

To resolve this you'll have to restart your Python process and ensure that you

    import tables

before you import projects like odo or into or blaze."""

from collections import namedtuple

EmptyHDFStoreDataset = namedtuple('EmptyHDFStoreDataset', 'parent,pathname,dshape')

@resource.register('hdfstore://.+', priority=11)
def resource_hdfstore(uri, datapath=None, dshape=None, **kwargs):
    # TODO:
    # 1. Support nested datashapes (e.g. groups)
    # 2. Try translating unicode to ascii?  (PyTables fails here)
    fn = uri.split('://')[1]
    try:
        f = pd.HDFStore(fn, **filter_kwargs(pd.HDFStore, kwargs))
    except RuntimeError as e:
        raise type(e)(pytables_h5py_explanation)

    if dshape is None:
        return f.get_storer(datapath) if datapath else f
    dshape = datashape.dshape(dshape)

    # Already exists, return it
    if datapath in f:
        return f.get_storer(datapath)

    # Need to create new dataset.
    # HDFStore doesn't support empty datasets, so we use a proxy object.
    return EmptyHDFStoreDataset(f, datapath, dshape)


@append.register((pd.io.pytables.Fixed, EmptyHDFStoreDataset), pd.DataFrame)
def append_dataframe_to_hdfstore(store, df, **kwargs):
    store.parent.append(store.pathname, df, append=True)
    return store.parent.get_storer(store.pathname)


@append.register((pd.io.pytables.Fixed, EmptyHDFStoreDataset),
                 chunks(pd.DataFrame))
def append_chunks_dataframe_to_hdfstore(store, c, **kwargs):
    parent = store.parent
    for chunk in c:
        parent.append(store.pathname, chunk)
    return parent.get_storer(store.pathname)


@append.register((pd.io.pytables.Fixed, EmptyHDFStoreDataset), object)
def append_object_to_hdfstore(store, o, **kwargs):
    return append(store, convert(chunks(pd.DataFrame), o, **kwargs), **kwargs)


ooc_types.add(pd.io.pytables.AppendableFrameTable)
