from __future__ import absolute_import, division, print_function

import datashape
from datashape import discover
from datashape.dispatch import dispatch
from into import append
from into.create import create
from into.cleanup import cleanup
from into.convert import convert, ooc_types
from into.drop import drop
from into.resource import resource, resource_matches
from into.chunks import chunks, Chunks
from into.utils import tmpfile
from into.numpy_dtype import dshape_to_pandas
from into.backends.hdf import HDFFile, HDFTable

from collections import Iterator
from contextlib import contextmanager
import os
import numpy as np
import tables
import pandas as pd
from pandas.io import pytables as hdf


@contextmanager
def ensure_indexing(t):
    """ turn off indexing for the scope of the operation """

    # turn off indexing
    t.table.autoindex = False

    # operate
    yield

    # reindex
    t.table.autoindex = True
    t.table.reindex_dirty()


class EmptyAppendableFrameTable(hdf.AppendableFrameTable):

    """
    represents an empty table, not yet actually created

    the current impl of HDFStore does not allow the actual
    creation of an empty table, so we use this holder until
    an append

    """
    @property
    def shape(self):
        return (0,)


@discover.register((EmptyAppendableFrameTable, hdf.AppendableFrameTable))
def discover_tables_node(t):
    if hasattr(t, 'dshape'):
        return t.dshape

    return datashape.from_numpy((t.shape,), t.dtype)


@discover.register(hdf.HDFStore)
def discover_tables_node(f):
    return discover(dict(f.items()))


@drop.register(hdf.AppendableFrameTable)
def drop_table(t):
    t.parent.remove(t.pathname)
    cleanup(t)


# hdf resource impl
@dispatch(hdf.HDFStore)
def pathname(f):
    return f.filename


@dispatch(hdf.HDFStore)
def dialect(f):
    return 'HDFStore'


@dispatch(hdf.HDFStore)
def get_table(f, datapath, dshape=None, **kwargs):

    assert datapath is not None

    node = f.get_node(datapath)
    if node is None:

        # create a new node
        f._handle.create_group('/', datapath.lstrip('/'), createparents=True)
        node = f.get_node(datapath)

    if getattr(node, 'table', None) is None:

        # create our stand-in table (with 0 dshape)
        s = EmptyAppendableFrameTable(f, node)
        s.set_object_info()
        if dshape is not None:
            s.dshape = datashape.dshape('0 * ' + str(dshape.measure))

        return s

    return f.get_storer(datapath)


@create.register(hdf.HDFStore, object)
def create_store(f, pathname):
    f.open()
    return f


@cleanup.register(hdf.HDFStore)
def cleanup_store(t):
    t.close()


@cleanup.register(hdf.AppendableFrameTable)
def cleanup_table(t):
    t.parent.close()


@append.register(hdf.HDFStore, hdf.HDFStore)
def append_object_to_hdfstore(s, data, **kwargs):
    """ this is essentially a gigantic append """
    raise NotImplementedError(
        "cannot append one store to another, pls specify datapaths")


@append.register((EmptyAppendableFrameTable, hdf.AppendableFrameTable), pd.DataFrame)
def append_frame_to_hdftable(s, data, datapath=None, **kwargs):
    """ append a single frame to a table, must have a datapath """

    p = s.parent
    if datapath is None:
        datapath = s.group._v_name

    p.append(datapath, data, data_columns=True)
    return p.get_storer(datapath)


@append.register((EmptyAppendableFrameTable, hdf.AppendableFrameTable), object)
def append_object_to_hdftable(t, data, **kwargs):
    """ append a single frame to a store """

    converted = convert(chunks(pd.DataFrame), data, **kwargs)
    return append_chunks_to_hdftable(t, converted, **kwargs)


@append.register((EmptyAppendableFrameTable, hdf.AppendableFrameTable), chunks(pd.DataFrame))
def append_chunks_to_hdftable(t, data, **kwargs):
    """
    append chunks to a store

    we have an existing empty table
    """
    data = iter(data)

    # the head element
    t = append_frame_to_hdftable(t, next(data), **kwargs)

    # the rest
    with ensure_indexing(t):
        for d in data:
            append_frame_to_hdftable(t, d, **kwargs)

    return t


def _use_sub_columns_selection(t, columns):
    # should we use an efficient sub-selection method
    if columns is None:
        return False

    n = t.ncols
    l = len(columns)

    return (l <= n / 2) & (l <= 4)


def _select_columns(t, key, **kwargs):
    # return a single column

    return t.read_column(key, **kwargs)


@convert.register(pd.DataFrame, hdf.AppendableFrameTable, cost=3.0)
def hdfstore_to_dataframe(t, where=None, columns=None, **kwargs):

    if where is None and columns is not None:

        # just select the columns
        # where is not currently support here
        if _use_sub_columns_selection(t, columns):
            return pd.concat([_select_columns(t, c, **kwargs) for c in columns],
                             keys=columns,
                             axis=1)

    return t.parent.select(t.group._v_name, where=where, columns=columns, **kwargs)


@convert.register(chunks(pd.DataFrame), hdf.AppendableFrameTable, cost=5.0)
def hdfstore_to_dataframe_chunks(t, chunksize=1e7, **kwargs):
    """ retrieve by chunks with the the embedded iterator """
    return chunks(pd.DataFrame)(hdfstore_to_dataframe_iterator(t, chunksize=chunksize, **kwargs))


@convert.register(Iterator, hdf.AppendableFrameTable, cost=5.0)
def hdfstore_to_dataframe_iterator(t, chunksize=1e7, **kwargs):
    """ return the embedded iterator """
    return t.parent.select(t.group._v_name, chunksize=chunksize, **kwargs)

# prioritize over native pytables


@resource.register('^(hdfstore://)?.+\.(h5|hdf5)', priority=12)
def resource_hdfstore(path, *args, **kwargs):
    path = resource_matches(path, 'hdfstore')
    return HDFStore(path, *args, **kwargs)


def HDFStore(path, datapath=None, dshape=None, **kwargs):
    """Create or open a ``hdf.HDFStore`` object.

    Parameters
    ----------
    path : str
        Path to a pandas HDFStore file
    datapath : str
        The name of the node in the file
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
    ...     t = HDFStore(filename, '/bar',
    ...                  dshape='var * {volume: float64, planet: string[10, "A"]}')
    ...     data = pd.DataFrame([(100.3, 'mars'), (100.42, 'jupyter')])
    ...     t.append(data)
    ...     t.select()  # doctest: +SKIP
    ...
    pd.DataFrame.from_records(array([(100.3, b'mars'), (100.42, b'jupyter')],
                              dtype=[('volume', '<f8'), ('planet', 'S10')]))
    """

    def create_as_empty(store, datapath=datapath, dshape=dshape):
        """ create and return an EmptyAppendableFrameTable """

        # dshape is ony required if the path does not exists
        if not dshape:
            store.close()
            raise ValueError("cannot create a HDFStore without a datashape")

        if isinstance(dshape, str):
            dshape = datashape.dshape(dshape)
        if dshape[0] == datashape.var:
            dshape = dshape.subshape[0]
        try:
            dtype = dshape_to_pandas(dshape)[0]
        except AttributeError:
            store.close()
            raise ValueError("need to pass a proper datashape to HDFStore")

        # no datapath, then return the store
        if datapath is None:
            return HDFFile(store)

        return HDFTable(HDFFile(store), datapath, dshape=dshape)

    if not os.path.exists(path):
        store = hdf.HDFStore(path, **kwargs)
        return create_as_empty(store=store)

    # inspect the store to make sure that we only handle HDFStores
    # otherwise defer to other resources
    store = hdf.HDFStore(path, **kwargs)

    try:
        store._path
    except AttributeError:
        store.close()
        raise NotImplementedError("not a hdfstore type")

    group = store.get_node(datapath)
    if group is None:
        return HDFFile(store, datapath=datapath)

    # further validation on the actual node
    try:
        group._v_attrs.pandas_type
    except AttributeError:
        store.close()
        raise NotImplementedError("not a hdfstore type")

    return HDFTable(HDFFile(store), datapath)


ooc_types |= set([hdf.AppendableFrameTable])
