from __future__ import absolute_import, division, print_function

from glob import glob
from .chunks import Chunks, chunks
from .resource import resource
from .utils import copydoc
from toolz import memoize, first
from datashape import discover, var
import os


class _Directory(Chunks):
    """ A directory of files on disk

    For typed containers see the ``Directory`` function which generates
    parametrized Directory classes.

    >>> from odo import CSV
    >>> c = Directory(CSV)('path/to/data/')  # doctest: +SKIP

    Normal use through resource strings

    >>> r = resource('path/to/data/*.csv')  # doctest: +SKIP
    >>> r = resource('path/to/data/')  # doctest: +SKIP


    """
    def __init__(self, path, **kwargs):
        self.path = path
        self.kwargs = kwargs

    def __iter__(self):
        return (resource(os.path.join(self.path, fn), **self.kwargs)
                    for fn in sorted(os.listdir(self.path)))


@memoize
@copydoc(_Directory)
def Directory(cls):
    """ Parametrized DirectoryClass """
    return type('Directory(%s)' % cls.__name__, (_Directory, chunks(cls)), {})


re_path_sep = os.path.sep
if re_path_sep == '\\':
    re_path_sep = '\\\\'

@discover.register(_Directory)
def discover_Directory(c, **kwargs):
    return var * discover(first(c)).subshape[0]


@resource.register('.+' + re_path_sep + '\*\..+', priority=15)
def resource_directory(uri, **kwargs):
    path = uri.rsplit(os.path.sep, 1)[0]
    try:
        one_uri = first(glob(uri))
    except (OSError, StopIteration):
        return _Directory(path, **kwargs)
    subtype = type(resource(one_uri, **kwargs))
    return Directory(subtype)(path, **kwargs)


@resource.register('.+' + re_path_sep, priority=9)
def resource_directory_with_trailing_slash(uri, **kwargs):
    try:
        one_uri = os.listdir(uri)[0]
    except (OSError, IndexError):
        return _Directory(uri, **kwargs)
    subtype = type(resource(one_uri, **kwargs))
    return Directory(subtype)(uri, **kwargs)
