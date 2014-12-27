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
    >>> # Using hdfstore
    """
    raise NotImplementedError("cleanup not implemented for type %r" %
                              type(rsrc).__name__)


@dispatch((str, unicode))
def cleanup(uri, **kwargs):
    data = resource(uri, **kwargs)
    cleanup(data)
