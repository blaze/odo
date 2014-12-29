"""

Provide a unified HDF5 interface with auto open/close
of resources.

Exposes the top-level HDFFile, HDFTable as the concrete implementation
class. These have top-level unified method signatures.

This module is in the spirit of sqlalchemy, where the impl of the HDF5
driver is a dialect.

"""


from __future__ import absolute_import, division, print_function
from contextlib import contextmanager

from datashape.dispatch import dispatch
from into import append, discover, convert, resource
from into.convert import ooc_types
from into.chunks import chunks
import pandas as pd

__all__ = ['HDFFile','HDFTable']

# provide some registration hooks for our implementations
@dispatch(object)
def pathname(f):
    """ return my pathname """
    raise NotImplementedError()

@dispatch(object)
def get_table(f):
    """ return a table from a passed string """
    raise NotImplementedError()

@dispatch(object)
def open_handle(f):
    """ return an open handle """
    raise NotImplementedError()

@dispatch(object)
def cleanup(f):
    """ cleanup """
    raise NotImplementedError()

class HDFFile(object):
    """ An interface to generic hdf objects

    Parameters
    ----------
    rsrc : rsrc
        a HDF resource of the File type
    datapath : str
        The name of the node in the file
    dshape : str or datashape.DataShape

    Returns
    -------
    t : HDFFile if datapath is not None
        HDFTable otherwise

    """

    def __init__(self, rsrc, datapath=None, dshape=None, **kwargs):
        self.rsrc = rsrc
        self.datapath = datapath
        self.dshape = dshape
        self.kwargs = kwargs

        # make sure our resoure is clean
        cleanup(self.rsrc)

        # set the pathname
        with self as handle:
            self.pathname = pathname(handle)

    @property
    def dialect(self):
        return self.rsrc.__class__.__name__

    def __str__(self):
        return "{klass} [{dialect}]: [path->{path}, datapath->{datapath}]".format(klass=self.__class__.__name__,
                                                                                  dialect=self.dialect,
                                                                                  path=self.pathname,
                                                                                  datapath=self.datapath)
    __repr__ = __str__

    def __contains__(self, key):
        """ node checking """
        return key in discover(self).names


    def get_table(self, datapath=None):
        """
        return the specified table

        if datapath is None, then use the default datapath
        raise ValueError if this is None
        """
        datapath = datapath or self.datapath
        if datapath is None:
            raise ValueError(
                "must specify a datapath in order to access a table in a hdf store")

        return HDFTable(self, datapath)

    def open_handle(self):
        return open_handle(self.rsrc)

    def cleanup(self):
        cleanup(self.rsrc)

    def __enter__(self):
        """ make sure our resource is open """
        return self.open_handle()

    def __exit__(self, *args):
        """ make sure our resource is closed """
        self.cleanup()

class HDFTable(object):
    """
    an abstract table representation in an HDFile

    Parameters
    ----------
    parent : the parent HDFFile
    datapath : str name of the datapath

    """

    def __init__(self, parent, datapath):
        self.parent = parent
        self.datapath = datapath

    def __str__(self):
        return "{klass} [{dialect}]: [{datapath}]".format(klass=self.__class__.__name__,
                                                          dialect=self.dialect,
                                                          datapath=self.datapath)
    __repr__ = __str__

    @property
    def dialect(self):
        return self.parent.dialect

    @property
    def pathname(self):
        return self.parent.pathname

    def __enter__(self):
        """ return the actual node in a open/close context manager """
        handle = self.parent.open_handle()
        return get_table(handle, datapath=self.datapath)

    def __exit__(self, *args):
        """ make sure our resource is closed """
        self.parent.cleanup()


@discover.register(HDFFile)
def discover_file(f):
    with f as handle:
        return discover(f.rsrc)

@discover.register(HDFTable)
def discover_table(t):
    with t as ot:
        return discover(ot)

@append.register(HDFFile, object)
def append_object_to_store(s, data, datapath=None, **kwargs):
    """ append a single object to store, must have a datapath """

    # we possible attached to the store from the original uri
    datapath = datapath or s.datapath
    if datapath is None:
        raise ValueError(
            "must specify a datapath in order to append to a hdf store")

    t = HDFTable(s, datapath)

    with t as handle:
        append(handle, data, datapath=datapath, **kwargs)
    return t

@append.register(HDFTable, object)
def append_object_to_table(t, data, **kwargs):
    """ append a single object to a table """

    with t as handle:
        append(handle, data, **kwargs)
    return t

@convert.register(pd.DataFrame, HDFTable, cost=3.0)
def hdftable_to_frame(t, **kwargs):
    with t as handle:
        return convert(pd.DataFrame, handle, **kwargs)

@convert.register(chunks(pd.DataFrame), HDFTable, cost=3.0)
def hdftable_to_chunks(t, **kwargs):
    with t as handle:
        return convert(chunks(pd.DataFrame), handle, **kwargs)

@dispatch(HDFFile)
def drop(f):
    cleanup(f)
    drop(f.rsrc)

@dispatch(HDFTable)
def drop(t):
    with t as handle:
        drop(handle)

@dispatch(HDFFile)
def cleanup(f):
    cleanup(f.rsrc)

@dispatch(HDFTable)
def cleanup(t):
    cleanup(t.parent)

ooc_types |= set([HDFTable])
