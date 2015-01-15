from __future__ import absolute_import, division, print_function

from toolz import memoize, first
from datashape import discover, var
from .utils import cls_name


class IterableOf(object):
    """ An Iterable of chunked data

    Iterates over chunks of in-memory data.  Contains an iterable or a function
    that returns an iterator.

    >>> c = IterableOf([[1, 2, 3], [4, 5, 6]])
    >>> next(iter(c))
    [1, 2, 3]

    For typed containers see the ``iterable`` function which generates
    parametrized ``IterableOf`` classes.

    >>> c = iterable(list)([[1, 2, 3], [4, 5, 6]])
    >>> next(iter(c))
    [1, 2, 3]

    >>> c.container.__name__
    'list'
    """

    def __init__(self, data):
        self.data = data

    def __iter__(self):
        if callable(self.data):
            return self.data()
        else:
            return iter(self.data)


def iterable(cls):
    """ Parametrized IterableOf Class """
    return type('iterable(%s)' % cls_name(cls), (IterableOf,), {'container': cls})

iterable.__doc__ = IterableOf.__doc__

iterable = memoize(iterable)


@discover.register(IterableOf)
def discover_chunks(c, **kwargs):
    return var * discover(first(c)).subshape[0]
