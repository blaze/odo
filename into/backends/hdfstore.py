from __future__ import absolute_import, division, print_function

import datashape
from datashape import discover
from datashape.dispatch import dispatch
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks, Chunks
from ..utils import tmpfile
from ..numpy_dtype import dshape_to_pandas

from contextlib import contextmanager
import os
import numpy as np
import tables
import pandas as pd
from pandas.io import pytables as hdf

__all__ = ['HDFStore']

@contextmanager
def ensure_indexing(t):
    """ turn off indexing for the scope of the operation """

    # turn off indexing
    t.table.autoindex=False

    # operate
    yield

    # reindex
    t.table.autoindex=True
    t.table.reindex_dirty()

def _is_empty(t):
    """ are we representing an empty table """
    return t.nrows == 1 and t.read().isnull().all().all()

def _replace_empty(t, data):
    """
    we have an existing table stored that represents an empty table
    if its there, replace it. return the new node
    """

    assert _is_empty(t)

    # indexing happens after the appending here
    name = t.group._v_name
    store = t.parent
    store.remove(name)
    store.append(name, data, format='table', data_columns=True)

    # we changed the node (even though it has the same name)
    # so make t point to it
    t.group = store.get_node(name)

    return t

@discover.register(hdf.Table)
def discover_tables_node(n):
    return datashape.from_numpy((n.shape,), n.dtype)

@append.register(hdf.AppendableFrameTable, pd.DataFrame)
def append_frame_to_hdfstore(t, data, **kwargs):
    """ append a single frame to a store """

    # see if we have our temp table stored, if so, remove it first
    if _is_empty(t):
        t = _replace_empty(t, data)

    # we have an existing table
    else:

        # append
        t.parent.append(t.group._v_name, data)

    return t

@append.register(hdf.AppendableFrameTable, chunks(pd.DataFrame))
def append_chunks_to_hdfstore(t, data, **kwargs):
    """
    append chunks to a store

    turn off indexing during this operation
    """

    with ensure_indexing(t):

        for chunk in data:
            t = append_frame_to_hdfstore(t, chunk, **kwargs)

    return t

@convert.register(pd.DataFrame, hdf.AppendableFrameTable, cost=3.0)
def hdfstore_to_dataframe(t, **kwargs):
    return t.parent.select(t.group._v_name,**kwargs)

@resource.register('.+\.h5')
def resource_hdfstore(path, datapath, **kwargs):
    return HDFStore(path, datapath, **kwargs)

def HDFStore(path, datapath, dshape=None, **kwargs):
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
    t : hdf.Table

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

    if not os.path.exists(path):

        # dshape is ony required if the path does not exists
        if not dshape:
            raise ValueError("cannot create a HDFStore without a datashape")

        if isinstance(dshape, str):
            dshape = datashape.dshape(dshape)
        if dshape[0] == datashape.var:
            dshape = dshape.subshape[0]
        dtype = dshape_to_pandas(dshape)[0]

        # create a temp table with a single row
        df = pd.DataFrame(dict([ (name,pd.Series(index=[0],dtype=dt)) for name,dt in dtype.items() ]))
        with hdf.HDFStore(path, **kwargs) as store:
            store.append(datapath,df,data_columns=True)

    return hdf.HDFStore(path, **kwargs).get_storer(datapath)

@dispatch(hdf.Table)
def drop(t):
    t.remove()

ooc_types |= set([hdf.Table])
