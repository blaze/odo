from __future__ import absolute_import, division, print_function

from datashape.dispatch import dispatch
from .resource import resource


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
    >>> from blaze import SQL, into
    >>> # create a table called 'tb', in memory
    >>> sql = SQL('sqlite:///:memory:', 'tb',
    ...           schema='{id: int64, value: float64, categ: string}')
    >>> data = [(1, 2.0, 'a'), (2, 3.0, 'b'), (3, 4.0, 'c')]
    >>> sql.extend(data)
    >>> into(list, sql)
    [(1, 2.0, 'a'), (2, 3.0, 'b'), (3, 4.0, 'c')]
    >>> sql.table.exists(sql.engine)
    True
    >>> drop(sql)
    >>> sql.table.exists(sql.engine)
    False
    """
    raise NotImplementedError("drop not implemented for type %r" %
                              type(rsrc).__name__)


@dispatch(basestring)
def drop(uri, **kwargs):
    data = resource(uri, **kwargs)
    drop(data)
