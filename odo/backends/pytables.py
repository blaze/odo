from __future__ import absolute_import, division, print_function

from datashape import discover
from datashape.dispatch import dispatch
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..utils import tmpfile

import os

import numpy as np
import tables

from toolz import first

import datashape

import shutil

__all__ = ['PyTables']


@discover.register((tables.Array, tables.Table))
def discover_tables_node(n):
    return datashape.from_numpy(n.shape, n.dtype)


@discover.register(tables.Node)
def discover_tables_node(n):
    return discover(n._v_children)  # subclasses dict


@discover.register(tables.File)
def discover_tables_node(f):
    return discover(f.getNode('/'))


@append.register((tables.Array, tables.Table), np.ndarray)
def numpy_to_pytables(t, x, **kwargs):
    t.append(x)
    return x


@append.register((tables.Array, tables.Table), object)
def append_h5py(dset, x, **kwargs):
    return append(dset, convert(chunks(np.ndarray), x, **kwargs), **kwargs)


@convert.register(np.ndarray, tables.Table, cost=3.0)
def pytables_to_numpy(t, **kwargs):
    return t[:]


@convert.register(chunks(np.ndarray), tables.Table, cost=3.0)
def pytables_to_numpy_chunks(t, chunksize=2**20, **kwargs):
    def load():
        for i in range(0, t.shape[0], chunksize):
            yield t[i: i + chunksize]
    return chunks(np.ndarray)(load)


def dtype_to_pytables(dtype):
    """ Convert NumPy dtype to PyTable descriptor

    Examples
    --------
    >>> from tables import Int32Col, StringCol, Time64Col
    >>> dt = np.dtype([('name', 'S7'), ('amount', 'i4'), ('time', 'M8[us]')])
    >>> dtype_to_pytables(dt)  # doctest: +SKIP
    {'amount': Int32Col(shape=(), dflt=0, pos=1),
     'name': StringCol(itemsize=7, shape=(), dflt='', pos=0),
     'time': Time64Col(shape=(), dflt=0.0, pos=2)}
    """
    d = {}
    for pos, name in enumerate(dtype.names):
        dt, _ = dtype.fields[name]
        if issubclass(dt.type, np.datetime64):
            tdtype = tables.Description({name: tables.Time64Col(pos=pos)}),
        else:
            tdtype = tables.descr_from_dtype(np.dtype([(name, dt)]))
        el = first(tdtype)
        getattr(el, name)._v_pos = pos
        d.update(el._v_colobjects)
    return d


def PyTables(path, datapath, dshape=None, **kwargs):
    """Create or open a ``tables.Table`` object.

    Parameters
    ----------
    path : str
        Path to a PyTables HDF5 file.
    datapath : str
        The name of the node in the ``tables.File``.
    dshape : str or datashape.DataShape
        DataShape to use to create the ``Table``.

    Returns
    -------
    t : tables.Table

    Examples
    --------
    >>> from odo.utils import tmpfile
    >>> # create from scratch
    >>> with tmpfile('.h5') as f:
    ...     t = PyTables(filename, '/bar',
    ...                  dshape='var * {volume: float64, planet: string[10, "A"]}')
    ...     data = [(100.3, 'mars'), (100.42, 'jupyter')]
    ...     t.append(data)
    ...     t[:]  # doctest: +SKIP
    ...
    array([(100.3, b'mars'), (100.42, b'jupyter')],
          dtype=[('volume', '<f8'), ('planet', 'S10')])
    """
    def possibly_create_table(filename, dtype):
        f = tables.open_file(filename, mode='a')
        try:
            if datapath not in f:
                if dtype is None:
                    raise ValueError('dshape cannot be None and datapath not'
                                     ' in file')
                else:
                    f.create_table('/', datapath.lstrip('/'), description=dtype)
        finally:
            f.close()

    if dshape:
        if isinstance(dshape, str):
            dshape = datashape.dshape(dshape)
        if dshape[0] == datashape.var:
            dshape = dshape.subshape[0]
        dtype = dtype_to_pytables(datashape.to_numpy_dtype(dshape))
    else:
        dtype = None

    if os.path.exists(path):
        possibly_create_table(path, dtype)
    else:
        with tmpfile('.h5') as filename:
            possibly_create_table(filename, dtype)
            shutil.copyfile(filename, path)
    return tables.open_file(path, mode='a').get_node(datapath)


@resource.register('pytables://.+', priority=11)
def resource_pytables(path, datapath, **kwargs):
    return PyTables(path[len('pytables://'):], datapath, **kwargs)


@dispatch((tables.Table, tables.Array))
def drop(t):
    t.remove()


@dispatch(tables.File)
def drop(f):
    f.close()
    os.remove(f.filename)


ooc_types |= set((tables.Table, tables.Array))
