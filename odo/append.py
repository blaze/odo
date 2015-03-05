from __future__ import absolute_import, division, print_function

from multipledispatch import Dispatcher
from datashape.dispatch import namespace
from .convert import convert


if 'append' not in namespace:
    namespace['append'] = Dispatcher('append')
append = namespace['append']


@append.register(object, object)
def append_not_found(a, b, **kwargs):
    """ Append one dataset on to another

    Examples
    --------

    >>> data = [1, 2, 3]
    >>> _ = append(data, (4, 5, 6))
    >>> data
    [1, 2, 3, 4, 5, 6]
    """
    raise NotImplementedError("Don't know how to append datasets of type "
            "%s on to type %s" % (type(b), type(a)))


@append.register(list, list)
def list_to_list(a, b, **kwargs):
    a.extend(b)
    return a


@append.register(list, object)
def object_to_list(a, b, **kwargs):
    append(a, convert(list, b))
    return a


@append.register(set, set)
def set_to_set(a, b, **kwargs):
    a.update(b)
    return a


@append.register(set, object)
def object_to_set(a, b, **kwargs):
    append(a, convert(set, b))
    return a
