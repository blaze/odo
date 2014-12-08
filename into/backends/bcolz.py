from __future__ import absolute_import, division, print_function

import bcolz
from bcolz import ctable, carray
import numpy as np
from toolz import keyfilter
from ..append import append
from ..convert import convert
from ..create import create
from ..resource import resource

keywords = ['rootdir']


@append.register((ctable, carray), np.ndarray)
def numpy_append_to_bcolz(a, b, **kwargs):
    a.append(b)
    return a


@append.register((ctable, carray), object)
def numpy_append_to_bcolz(a, b, **kwargs):
    return append(a, convert(np.ndarray, b), **kwargs)


@convert.register(ctable, np.ndarray)
def convert_numpy_to_bcolz_ctable(x, **kwargs):
    return ctable(x, **keyfilter(keywords.__contains__, kwargs))


@convert.register(carray, np.ndarray)
def convert_numpy_to_bcolz_carray(x, **kwargs):
    return carray(x, **keyfilter(keywords.__contains__, kwargs))


@convert.register(np.ndarray, (carray, ctable))
def convert_bcolz_to_numpy(x, **kwargs):
    return x[:]
