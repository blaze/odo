from __future__ import absolute_import, division, print_function

from glob import glob
from .chunks import Chunks
from .resource import resource
from toolz import memoize, first
from datashape import discover, var
import os


class _Directory(Chunks):
    """ A directory of files on disk

    >>> d = _Directory('path/to/data')

    For typed containers see the ``Directory`` function which generates
    parametrized Directory classes.

    >>> c = Directory(CSV)('path/to/data')
    """

    def __init__(self, path, **kwargs):
        self.path = path
        self.kwargs = kwargs

    def __iter__(self):
        return (resource(os.path.join(self.path, fn), **self.kwargs)
                    for fn in sorted(os.listdir(self.path)))


def Directory(cls):
    """ Parametrized DirectoryClass """
    return type('Directory(%s)' % cls.__name__, (_Directory,), {'container': cls})

Directory.__doc__ = Directory.__doc__

Directory = memoize(Directory)


@discover.register(_Directory)
def discover_Directory(c, **kwargs):
    return var * discover(first(c)).subshape[0]
