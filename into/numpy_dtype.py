from __future__ import absolute_import, division, print_function

import numpy as np
from datashape import *
from datashape.predicates import isscalar, isnumeric

def unit_to_dtype(ds):
    """

    >>> unit_to_dtype('int32')
    dtype('int32')
    >>> unit_to_dtype('float64')
    dtype('float64')
    >>> unit_to_dtype('?int64')
    dtype('float64')
    >>> unit_to_dtype('string')
    dtype('O')
    >>> unit_to_dtype('?datetime')
    dtype('<M8[us]')
    """
    if isinstance(ds, str):
        ds = dshape(ds)
    if isinstance(ds, DataShape):
        ds = ds.measure
    if isinstance(ds, Option) and isscalar(ds) and isnumeric(ds):
        return unit_to_dtype(str(ds).replace('int', 'float').replace('?', ''))
    if isinstance(ds, Option) and ds.ty in (date_, datetime_, string):
        ds = ds.ty
    if ds == string:
        return np.dtype('O')
    return to_numpy_dtype(ds)


def dshape_to_numpy(ds):
    """

    >>> dshape_to_numpy('int32')
    dtype('int32')
    >>> dshape_to_numpy('?int32')
    dtype('float32')

    >>> dshape_to_numpy('{name: string[5, "ascii"], amount: ?int32}')
    dtype([('name', 'S5'), ('amount', '<f4')])

    >>> dshape_to_numpy('(int32, float32)')
    dtype([('f0', '<i4'), ('f1', '<f4')])
    """
    if isinstance(ds, str):
        ds = dshape(ds)
    if isinstance(ds, DataShape):
        ds = ds.measure
    if isrecord(ds):
        return np.dtype([(str(name), unit_to_dtype(typ))
            for name, typ in zip(ds.names, ds.types)])
    if isinstance(ds, Tuple):
        return np.dtype([('f%d' % i, unit_to_dtype(typ))
            for i, typ in enumerate(ds.parameters[0])])
    else:
        return unit_to_dtype(ds)


def dshape_to_pandas(ds):
    """

    >>> dshape_to_pandas('{a: int32}')
    ({'a': dtype('int32')}, [])

    >>> dshape_to_pandas('{a: int32, when: datetime}')
    ({'a': dtype('int32')}, ['when'])

    >>> dshape_to_pandas('{a: ?int64}')
    ({'a': dtype('float64')}, [])
    """
    if isinstance(ds, str):
        ds = dshape(ds)
    if isinstance(ds, DataShape) and len(ds) == 1:
        ds = ds[0]

    dtypes = dict((name, unit_to_dtype(typ))
                  for name, typ in ds.measure.dict.items()
                  if not 'date' in str(typ))

    datetimes = [name for name, typ in ds.measure.dict.items()
                    if 'date' in str(typ)]

    return dtypes, datetimes
