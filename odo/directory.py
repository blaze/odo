from __future__ import absolute_import, division, print_function

import os
import re

from glob import glob
from pprint import pformat

from toolz import memoize, first
from datashape import discover, var

from .chunks import Chunks
from .resource import resource


class _Directory(Chunks):
    """ A directory of files on disk

    For typed containers see the ``Directory`` function which generates
    parametrized Directory classes.

    Parameters
    ----------
    path : str
        An existing directory or glob pattern
    kwargs : dict
        Additional keyword arguments that will be passed to the call to
        resource when iterating over a directory

    Examples
    --------
    >>> from odo import CSV
    >>> c = Directory(CSV)('path/to/data/') # doctest: +SKIP
    >>> c # doctest: +SKIP
    Directory(CSV)(path=..., pattern='*')

    Normal use through resource strings

    >>> r = resource('path/to/data/*.csv')  # doctest: +SKIP
    Directory(CSV)(path=..., pattern='*.csv')
    >>> r = resource('path/to/data/')  # doctest: +SKIP
    Directory()(path=..., pattern='*')
    """
    def __init__(self, path, **kwargs):
        path = os.path.abspath(path)
        if os.path.isdir(path):
            self.pattern = '*'
        else:
            path, self.pattern = os.path.split(path)
        self.path = os.path.abspath(path)
        self.kwargs = kwargs

    @property
    def glob(self):
        return os.path.join(self.path, self.pattern)

    def __iter__(self):
        return (resource(fn, **self.kwargs) for fn in sorted(glob(self.glob)))

    def __repr__(self):
        return '%s(glob=%s)' % (type(self).__name__, self.glob)


def Directory(cls):
    """ Parametrized DirectoryClass """
    return type('Directory(%s)' % cls.__name__, (_Directory,),
                {'container': cls})

Directory.__doc__ = Directory.__doc__

Directory = memoize(Directory)


re_path_sep = os.path.sep
if re_path_sep == '\\':
    re_path_sep = '\\\\'

@discover.register(_Directory)
def discover_Directory(c, **kwargs):
    return var * discover(first(c)).subshape[0]


@resource.register(r'.+%s.*(?:(?:\[!?.+\])|\?|\*)+.+' % re_path_sep, priority=18)
def resource_directory(uri, **kwargs):
    try:
        one_uri = first(glob(uri))
    except (OSError, StopIteration):
        return _Directory(uri, **kwargs)
    subtype = type(resource(one_uri, **kwargs))
    return Directory(subtype)(uri, **kwargs)


@resource.register('.+' + re_path_sep, priority=9)
def resource_directory_with_trailing_slash(uri, **kwargs):
    try:
        one_uri = os.listdir(uri)[0]
    except (OSError, IndexError):
        return _Directory(uri, **kwargs)
    subtype = type(resource(one_uri, **kwargs))
    return Directory(subtype)(uri, **kwargs)
