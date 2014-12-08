from __future__ import absolute_import, division, print_function

from toolz import merge
from multipledispatch import Dispatcher
from .convert import convert
from .append import append
from .resource import resource

into = Dispatcher('into')


@into.register(type, object)
def into_type(a, b, **kwargs):
    return convert(a, b)


@into.register(object, object)
def into_object(a, b, **kwargs):
    return append(a, b)


@into.register(str, object)
def into_string(uri, b, **kwargs):
    if 'dshape' not in kwargs:
        kwargs['dshape'] = discover(b)
    a = resource(uri, **kwargs)
    return into(a, b, **kwargs)


@into.register(object)
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
