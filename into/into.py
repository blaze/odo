from __future__ import absolute_import, division, print_function

from toolz import merge
from multipledispatch import Dispatcher
from .convert import convert
from .append import append

into = Dispatcher('into')


@into.register(type, object)
def into_type(a, b, **kwargs):
    return convert(a, b)


@into.register(object, object)
def into_object(a, b, **kwargs):
    return append(a, b)


@into.register(object)
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
