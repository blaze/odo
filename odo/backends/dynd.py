from __future__ import absolute_import, division, print_function

from ..convert import convert
from ..append import append
from ..create import create
from datashape import discover, dshape, DataShape, Record, Tuple
import datashape
import numpy as np
from dynd import nd

@convert.register(np.ndarray, nd.array, cost=1000.1)
def dynd_to_numpy(x, **kwargs):
    return nd.as_numpy(x, allow_copy=True)


@convert.register(nd.array, np.ndarray, cost=1000.8)
def numpy_to_dynd(x, **kwargs):
    return nd.array(x, type=str(discover(x)))


@convert.register(list, nd.array, cost=100.0)
def dynd_to_list(x, **kwargs):
    return nd.as_py(x, tuple=True)


@convert.register(nd.array, list, cost=90.0)
def list_to_dynd(L, **kwargs):
    ds = kwargs['dshape']
    if isinstance(ds.measure, Tuple):
        measure = Record([['f%d'%i, typ] for i, typ in
            enumerate(ds.measure.parameters[0])])
        ds = DataShape(*(ds.shape + (measure,)))
    return nd.array(L, dtype=str(ds))


@create.register(nd.array)
def create_dynd_array(x, dshape=None):
    return nd.empty(str(dshape))


@discover.register(nd.array)
def discover_dynd_array(x, **kwargs):
    return dshape(str(nd.type_of(x)))
