from __future__ import absolute_import, division, print_function

from toolz import memoize, first
from datashape import discover, var
from .utils import cls_name


class Chunks(object):
    """ An Iterable of chunked data

    Iterates over chunks of in-memory data.  Contains an iterable or a function
    that returns an iterator.

    >>> c = Chunks([[1, 2, 3], [4, 5, 6]])
    >>> next(iter(c))
    [1, 2, 3]

    For typed containers see the ``chunks`` function which generates
    parametrized Chunks classes.

    >>> c = chunks(list)([[1, 2, 3], [4, 5, 6]])
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


def chunks(cls):
    """ Parametrized Chunks Class """
    return type('chunks(%s)' % cls_name(cls), (Chunks,), {'container': cls})

chunks.__doc__ = Chunks.__doc__

chunks = memoize(chunks)


@discover.register(Chunks)
def discover_chunks(c, **kwargs):
    return var * discover(first(c)).subshape[0]
