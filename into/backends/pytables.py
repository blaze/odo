from __future__ import absolute_import, division, print_function

import os
import tables
import numpy as np
import shutil
from toolz import first
from collections import Iterator

import datashape
from datashape import discover
from datashape.dispatch import dispatch
from into import append
from into.convert import convert, ooc_types
from into.resource import resource, resource_matches
from into.chunks import chunks, Chunks
from into.utils import tmpfile
from into.backends.hdf import HDFFile, HDFTable, full_node_path


@discover.register((tables.Array, tables.Table))
def discover_tables_node(n):
    return datashape.from_numpy(n.shape, n.dtype)


@discover.register(tables.Node)
def discover_tables_node(n):
    return discover(n._v_children)  # subclasses dict


@discover.register(tables.File)
def discover_tables_node(f):
    return discover(f.getNode('/'))


@append.register((tables.Array, tables.Table), object)
def numpy_to_pytables(t, x, **kwargs):
    x = convert(np.ndarray, x, **kwargs)
    t.append(x)
    return x


@append.register((tables.Array, tables.Table), chunks(np.ndarray))
def append_chunks_to_pytables(t, x, **kwargs):
    for item in x:
        numpy_to_pytables(t,  item, **kwargs)
    return t


@convert.register(np.ndarray, tables.Table, cost=3.0)
def pytables_to_numpy(t, **kwargs):
    return t[:]


@convert.register(chunks(np.ndarray), tables.Table, cost=3.0)
def pytables_to_numpy_chunks(t, chunksize=2 ** 20, **kwargs):
    return chunks(np.ndarray)(pytables_to_numpy_iterator(t, chunksize=chunksize, **kwargs))

@convert.register(Iterator, tables.Table, cost=5.0)
def pytables_to_numpy_iterator(t, chunksize=1e7, **kwargs):
    """ return the embedded iterator """
    for i in range(0, t.shape[0], int(chunksize)):
        yield t[i: i + chunksize]

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


def PyTables(path, datapath=None, dshape=None, **kwargs):
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
    t : HDFFile or HDFTable if datapath is provided

    Examples
    --------
    >>> from into.utils import tmpfile
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
    def create_table_or_file(f, datapath=datapath, dshape=dshape):

        if dshape:

            # dshape is ony required if the path does not exists
            if not dshape:
                f.close()
                raise ValueError(
                    "cannot create a HDFStore without a datashape")

            if isinstance(dshape, str):
                dshape = datashape.dshape(dshape)
            if dshape[0] == datashape.var:
                dshape = dshape.subshape[0]

            # if we have a non-convertible dshape
            # then report as a ValueError
            try:
                dshape = dtype_to_pytables(datashape.to_numpy_dtype(dshape))
            except NotImplementedError as e:
                f.close()
                raise ValueError(e)

        # no datapath, then return the store
        if datapath is None:
            return HDFFile(f)

        # create the table if needed
        try:
            node = f.get_node(datapath)
        except tables.NoSuchNodeError:

            try:
                # create a new node
                f.create_table(
                    '/', datapath.lstrip('/'), description=dshape, createparents=True)
            except:
                f.close()
                raise

        return HDFTable(HDFFile(f), datapath)

    if not os.path.exists(path):
        f = tables.open_file(path, mode='a')
        return create_table_or_file(f, datapath, dshape)

    f = tables.open_file(path, mode='a')

    # validate that we have a PyTables file
    try:
        f.format_version
    except AttributeError:
        f.close()
        raise NotImplementedError

    return create_table_or_file(f, datapath, dshape)


@resource.register('^(pytables://)?.+\.(h5|hdf5)', priority=11.0)
def resource_pytables(path, *args, **kwargs):
    path = resource_matches(path, 'pytables')
    return PyTables(path, *args, **kwargs)


@dispatch((tables.Table, tables.Array))
def drop(t):
    t.remove()


@dispatch(tables.File)
def drop(f):
    cleanup(f)
    os.remove(f.filename)

# hdf resource impl


@dispatch(tables.File)
def pathname(f):
    return f.filename


@dispatch(tables.File)
def dialect(f):
    return 'PyTables'


@dispatch(tables.File)
def get_table(f, datapath):

    assert datapath is not None
    return f.get_node(full_node_path(datapath))


@dispatch(tables.File)
def open_handle(f):
    f.close()
    return tables.open_file(f.filename, mode='a')


@dispatch(tables.File)
def cleanup(f):
    f.close()


@dispatch((tables.Table, tables.Group))
def cleanup(f):
    f._v_file.close()

ooc_types |= set((tables.Table, tables.Array))
