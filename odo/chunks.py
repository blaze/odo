from __future__ import absolute_import, division, print_function

from collections import Iterator

from toolz import memoize, first, peek
from datashape import discover, var
from .utils import cls_name, copydoc
from dask.threaded import get as dsk_get


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
        elif (isinstance(self.data, list) and
              len(self.data) and
              callable(self.data[0])):
            # If this is a set of callables, evaluate
            # them using dask before returning an iterator for them
            p = []
            dsk = {}
            for i, f in enumerate(self.data):
                dsk['p%d'%i] = (f,)
                p.append('p%d'%i)
            self.data = dsk_get(dsk, p)
        return iter(self.data)


@memoize
@copydoc(Chunks)
def chunks(cls):
    """ Parametrized Chunks Class """
    return type('chunks(%s)' % cls_name(cls), (Chunks,), {'container': cls})


@discover.register(Chunks)
def discover_chunks(c, **kwargs):
    data = c.data
    if isinstance(data, Iterator):
        fst, c.data = peek(data)
    else:
        fst = first(c)
    return var * discover(fst).subshape[0]
