from __future__ import absolute_import, division, print_function

from toolz import merge
from multipledispatch import Dispatcher
from .convert import convert
from .append import append
from .resource import resource
from datashape import discover, var
from datashape.dispatch import namespace
from datashape.predicates import isdimension


if 'into' not in namespace:
    namespace['into'] = Dispatcher('into')
into = namespace['into']


@into.register(type, object)
def into_type(a, b, **kwargs):
    try:
        if 'dshape' not in kwargs:
            kwargs['dshape'] = discover(b)
    except NotImplementedError:
        pass
    return convert(a, b, **kwargs)


@into.register(object, object)
def into_object(a, b, **kwargs):
    """ Push one dataset into another

    Examples
    --------

    >>> # Convert things into new things
    >>> L = into(list, (1, 2, 3))
    >>> L
    [1, 2, 3]

    >>> # Add things onto existing things
    >>> _ = into(L, (4, 5, 6))
    >>> L
    [1, 2, 3, 4, 5, 6]

    >>> # Specify things with strings
    >>> into('myfile.csv', [('Alice', 1), ('Bob', 2)])  # doctest: +SKIP

    See Also
    --------

    into.convert.convert    - Convert things into new things
    into.append.append      - Add things onto existing things
    into.resource.resource  - Specify things with strings
    """
    try:
        if 'dshape' not in kwargs:
            kwargs['dshape'] = discover(b)
    except NotImplementedError:
        pass
    return append(a, b, **kwargs)


@into.register(str, object)
def into_string(uri, b, **kwargs):
    try:
        if 'dshape' not in kwargs:
            ds = discover(b)
            if isdimension(ds[0]):
                ds = var * ds.subshape[0]
            kwargs['dshape'] = ds
    except NotImplementedError:
        pass
    a = resource(uri, **kwargs)
    return into(a, b, **kwargs)


@into.register((type, str), str)
def into_string_string(a, b, **kwargs):
    r = resource(b, **kwargs)
    return into(a, r, **kwargs)


@into.register(object)
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
