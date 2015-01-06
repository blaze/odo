"""

define a dispatch object for cleaning up a backend
called on-demand or with a into(object, str) resource call

"""


from __future__ import absolute_import, division, print_function

from datashape.dispatch import dispatch
from .resource import resource
from .compatibility import unicode


@dispatch(object)
def cleanup(rsrc):
    """ Cleanup a resource

    Parameters
    ----------
    rsrc : tables.Table, hdfstore.AppendableFrameTable, h5py.Dataset
        A resource that will be cleaned up.

    Examples
    --------
    >>> from pandas import DataFrame
    >>> df = DataFrame({'A' : [1,2], 'B' : ['foo','bar']})
    >>> r = resource('foo.h5',dshape=discover(df))
    >>> cleanup(r)
    """
    pass
