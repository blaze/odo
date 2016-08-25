from __future__ import absolute_import, division, print_function

import numpy as np
from datashape import dshape, DataShape, Option, DateTime, string, TimeDelta
from datashape import Date, to_numpy_dtype, Tuple, String, Decimal
from datashape.predicates import isscalar, isnumeric, isrecord


def unit_to_dtype(ds):
    """ Convert a datashape Unit instance into a numpy dtype

    Parameters
    ----------
    ds : DataShape
        The DataShape instance to convert

    Returns
    -------
    np.dtype

    Examples
    --------
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
        if isinstance(ds.ty, Decimal):
            str_np_dtype = str(ds.ty.to_numpy_dtype()).replace('int', 'float')
            if str_np_dtype == 'float8':  # not a valid dtype, so increase
                str_np_dtype = 'float16'
            return unit_to_dtype(str_np_dtype)
        return unit_to_dtype(str(ds).replace('int', 'float').replace('?', ''))
    if isinstance(ds, Option) and isinstance(
        ds.ty, (Date, DateTime, String, TimeDelta)
    ):
        ds = ds.ty
    if ds == string:
        return np.dtype('O')
    return to_numpy_dtype(ds)


def dshape_to_numpy(ds):
    """ Convert a datashape to a NumPy dtype

    Parameters
    ----------
    ds : DataShape
        The DataShape instance to convert

    Returns
    -------
    np.dtype

    Examples
    --------
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
        return np.dtype([
            (str(name), unit_to_dtype(typ))
            for name, typ in zip(ds.names, ds.types)
        ])
    if isinstance(ds, Tuple):
        return np.dtype([
            ('f%d' % i, unit_to_dtype(typ))
            for i, typ in enumerate(ds.parameters[0])
        ])
    else:
        return unit_to_dtype(ds)


def dshape_to_pandas(ds):
    """ Convert a datashape to a pair of
    ``({name1: dtype1, name2: dtype2, ...}, [datecol1, datecol2, ...])``

    Parameters
    ----------
    ds : DataShape
        The DataShape instance to convert

    Returns
    -------
    ({str: np.dtype}, [str])

    Examples
    --------
    >>> dshape_to_pandas('{a: int32}')  # doctest: +SKIP
    ({'a': dtype('int32')}, [])

    >>> dshape_to_pandas('{a: int32, when: datetime}')  # doctest: +SKIP
    ({'a': dtype('int32')}, ['when'])

    >>> dshape_to_pandas('{a: ?int64}')  # doctest: +SKIP
    ({'a': dtype('float64')}, [])
    """
    if isinstance(ds, str):
        ds = dshape(ds)
    if isinstance(ds, DataShape) and len(ds) == 1:
        ds = ds[0]

    dtypes = {
        name: (
            np.dtype('object')
            if isinstance(typ, String) else unit_to_dtype(typ)
        )
        for name, typ in ds.measure.dict.items() if 'date' not in str(typ)
    }
    datetimes = [
        name for name, typ in ds.measure.dict.items() if 'date' in str(typ)
    ]

    return dtypes, datetimes
