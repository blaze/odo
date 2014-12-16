from __future__ import absolute_import, division, print_function

import bcolz
import os
from bcolz import ctable, carray
import numpy as np
from toolz import keyfilter
import datashape
from datashape import discover
from ..append import append
from ..convert import convert, ooc_types
from ..create import create
from ..resource import resource
from ..chunks import chunks, Chunks

keywords = ['rootdir']


@discover.register((ctable, carray))
def discover_bcolz(c, **kwargs):
    return datashape.from_numpy(c.shape, c.dtype)


@append.register((ctable, carray), np.ndarray)
def numpy_append_to_bcolz(a, b, **kwargs):
    a.append(b)
    a.flush()
    return a


@append.register((ctable, carray), object)
def numpy_append_to_bcolz(a, b, **kwargs):
    return append(a, convert(np.ndarray, b, **kwargs), **kwargs)


@convert.register(ctable, np.ndarray, cost=2.0)
def convert_numpy_to_bcolz_ctable(x, **kwargs):
    return ctable(x, **keyfilter(keywords.__contains__, kwargs))


@convert.register(carray, np.ndarray, cost=2.0)
def convert_numpy_to_bcolz_carray(x, **kwargs):
    return carray(x, **keyfilter(keywords.__contains__, kwargs))


@convert.register(np.ndarray, (carray, ctable), cost=1.0)
def convert_bcolz_to_numpy(x, **kwargs):
    return x[:]


@append.register((carray, ctable), chunks(np.ndarray))
def append_carray_with_chunks(a, c, **kwargs):
    for chunk in c:
        append(a, chunk)
    a.flush()
    return a


@convert.register(chunks(np.ndarray), (ctable, carray), cost=1.2)
def bcolz_to_numpy_chunks(x, chunksize=2**20, **kwargs):
    def load():
        first_n = min(1000, chunksize)
        first = x[:first_n]
        yield first
        for i in range(first_n, x.shape[0], chunksize):
            yield x[i: i + chunksize]
    return chunks(np.ndarray)(load)


@resource.register('.*\.bcolz/?')
def resource_bcolz(uri, dshape=None, **kwargs):
    if os.path.exists(uri):
        return ctable(rootdir=uri)
    else:
        if not dshape:
            raise ValueError("Must specify either existing bcolz directory or"
                    "valid datashape")
        dshape = datashape.dshape(dshape)

        dt = datashape.to_numpy_dtype(dshape)
        x = np.empty(shape=(0,), dtype=dt)

        if datashape.predicates.isrecord(dshape.measure):
            return ctable(x, rootdir=uri, **keyfilter(keywords.__contains__, kwargs))
        else:
            return carray(x, rootdir=uri, **keyfilter(keywords.__contains__, kwargs))


ooc_types |= set((carray, ctable))
