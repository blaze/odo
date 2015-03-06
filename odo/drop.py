from __future__ import absolute_import, division, print_function

from datashape.dispatch import dispatch
from .resource import resource
from .compatibility import unicode


@dispatch(object)
def drop(rsrc):
    """Remove a resource.

    Parameters
    ----------
    rsrc : CSV, SQL, tables.Table, pymongo.Collection
        A resource that will be removed. For example, calling ``drop(csv)`` will
        delete the CSV file.

    Examples
    --------
    >>> # Using SQLite
    >>> from odo import resource, odo
    >>> # create a table called 'tb', in memory
    >>> from datashape import dshape
    >>> ds = dshape('var * {name: string, amount: int}')
    >>> sql = resource('sqlite:///:memory:::tb', dshape=ds)
    >>> odo([('Alice', 100), ('Bob', 200)], sql)
    >>> odo(sql, list)
    [('Alice', 100), ('Bob', 200)]
    >>> drop(sql)
    """
    raise NotImplementedError("drop not implemented for type %r" %
                              type(rsrc).__name__)


@dispatch((str, unicode))
def drop(uri, **kwargs):
    data = resource(uri, **kwargs)
    drop(data)
