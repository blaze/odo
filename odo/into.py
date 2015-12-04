from __future__ import absolute_import, division, print_function

import functools

from toolz import merge

from multipledispatch import Dispatcher

from .convert import convert
from .append import append
from .resource import resource
from .utils import ignoring

import datashape
from datashape import discover
from datashape.dispatch import namespace
from datashape.predicates import isdimension

from .compatibility import unicode
from pandas import DataFrame, Series
from numpy import ndarray

not_appendable_types = DataFrame, Series, ndarray, tuple

__all__ = 'into',


if 'into' not in namespace:
    namespace['into'] = Dispatcher('into')

into = namespace['into']


def validate(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        dshape = kwargs.pop('dshape', None)
        if isinstance(dshape, (str, unicode)):
            dshape = datashape.dshape(dshape)
        if dshape is not None and not isinstance(dshape, datashape.DataShape):
            raise TypeError('dshape argument is not an instance of DataShape')
        kwargs['dshape'] = dshape
        return f(*args, **kwargs)
    return wrapped


@into.register(type, object)
@validate
def into_type(a, b, dshape=None, **kwargs):
    with ignoring(NotImplementedError):
        if dshape is None:
            dshape = discover(b)
    return convert(a, b, dshape=dshape, **kwargs)


@into.register(object, object)
@validate
def into_object(target, source, dshape=None, **kwargs):
    """ Push one dataset into another

    Parameters
    ----------

    source: object or string
        The source of your data.  Either an object (e.g. DataFrame),
    target: object or string or type
        The target for where you want your data to go.
        Either an object, (e.g. []), a type, (e.g. list)
        or a string (e.g. 'postgresql://hostname::tablename'
    raise_on_errors: bool (optional, defaults to False)
        Raise exceptions rather than reroute around them
    **kwargs:
        keyword arguments to pass through to conversion functions.

    Examples
    --------

    >>> L = into(list, (1, 2, 3))  # Convert things into new things
    >>> L
    [1, 2, 3]

    >>> _ = into(L, (4, 5, 6))  # Append things onto existing things
    >>> L
    [1, 2, 3, 4, 5, 6]

    >>> into('myfile.csv', [('Alice', 1), ('Bob', 2)])  # doctest: +SKIP

    Explanation
    -----------

    We can specify data with a Python object like a ``list``, ``DataFrame``,
    ``sqlalchemy.Table``, ``h5py.Dataset``, etc..

    We can specify data with a string URI like ``'myfile.csv'``,
    ``'myfiles.*.json'`` or ``'sqlite:///data.db::tablename'``.  These are
    matched by regular expression.  See the ``resource`` function for more
    details on string URIs.

    We can optionally specify datatypes with the ``dshape=`` keyword, providing
    a datashape.  This allows us to be explicit about types when mismatches
    occur or when our data doesn't hold the whole picture.  See the
    ``discover`` function for more information on ``dshape``.

    >>> ds = 'var * {name: string, balance: float64}'
    >>> into('accounts.json', [('Alice', 100), ('Bob', 200)], dshape=ds)  # doctest: +SKIP

    We can optionally specify keyword arguments to pass down to relevant
    conversion functions.  For example, when converting a CSV file we might
    want to specify delimiter

    >>> into(list, 'accounts.csv', has_header=True, delimiter=';')  # doctest: +SKIP

    These keyword arguments trickle down to whatever function ``into`` uses
    convert this particular format, functions like ``pandas.read_csv``.

    See Also
    --------

    into.resource.resource  - Specify things with strings
    datashape.discover      - Get datashape of data
    into.convert.convert    - Convert things into new things
    into.append.append      - Add things onto existing things
    """
    if isinstance(source, (str, unicode)):
        source = resource(source, dshape=dshape, **kwargs)
    if type(target) in not_appendable_types:
        raise TypeError('target of %s type does not support in-place append' % type(target))
    with ignoring(NotImplementedError):
        if dshape is None:
            dshape = discover(source)
    return append(target, source, dshape=dshape, **kwargs)


@into.register((str, unicode), object)
@validate
def into_string(uri, b, dshape=None, **kwargs):
    if dshape is None:
        dshape = discover(b)

    resource_ds = 0 * dshape.subshape[0] if isdimension(dshape[0]) else dshape

    a = resource(uri, dshape=resource_ds, expected_dshape=dshape, **kwargs)
    return into(a, b, dshape=dshape, **kwargs)


@into.register((type, (str, unicode)), (str, unicode))
@validate
def into_string_string(a, b, **kwargs):
    return into(a, resource(b, **kwargs), **kwargs)


@into.register(object)
@validate
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
