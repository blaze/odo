from __future__ import absolute_import, division, print_function

from multipledispatch import Dispatcher
import datashape
import numpy as np

create = Dispatcher('create')


@create.register(type)
def create_type(cls, **kwargs):
    if 'dshape' in kwargs:
        kwargs['dshape'] = datashape.dshape(kwargs['dshape'])
    func = create.dispatch(cls)
    return func(cls, **kwargs)


@create.register(np.ndarray)
def create_np_ndarray(_, dshape=None, **kwargs):
    shape, dtype = datashape.to_numpy(dshape)
    return np.empty(shape=shape, dtype=dtype)
