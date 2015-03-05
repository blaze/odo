from __future__ import absolute_import, division, print_function
from toolz import memoize
from .drop import drop

class _Temp(object):
    """ Temporary version of persistent storage

    Calls ``drop`` on object at garbage collection

    This is a parametrized type, so call it on types to make new types

    >>> from odo import Temp, CSV
    >>> csv = Temp(CSV)('/tmp/myfile.csv', delimiter=',')
    """
    def __del__(self):
        drop(self)


def Temp(cls):
    """ Parametrized Chunks Class """
    return type('Temp(%s)' % cls.__name__, (_Temp, cls), {'persistent_type': cls})

Temp.__doc__ = _Temp.__doc__

Temp = memoize(Temp)
