from __future__ import absolute_import, division, print_function

from ..convert import convert
from ..append import append
from ..create import create
from datashape import discover, dshape
import datashape
import numpy as np
from dynd import nd

@convert.register(np.ndarray, nd.array)
def dynd_to_numpy(x, **kwargs):
    return nd.as_numpy(x, allow_copy=True)


@convert.register(nd.array, np.ndarray)
def numpy_to_dynd(x, **kwargs):
    return nd.array(x, type=str(discover(x)))


@convert.register(list, nd.array)
def dynd_to_list(x, **kwargs):
    return nd.as_py(x)


@convert.register(nd.array, list)
def list_to_dynd(L, **kwargs):
    ds = kwargs['dshape']
    return nd.array(L, dtype=str(ds))


@create.register(nd.array)
def create_dynd_array(x, dshape=None):
    return nd.empty(str(dshape))


@discover.register(nd.array)
def discover_dynd_array(x, **kwargs):
    return dshape(str(nd.type_of(x)))
