from __future__ import absolute_import, division, print_function

from toolz import merge
from multipledispatch import Dispatcher
from .convert import convert
from .append import append
from .resource import resource
from .utils import ignoring
from datashape import discover, var
from datashape.dispatch import namespace
from datashape.predicates import isdimension
from .compatibility import unicode


if 'into' not in namespace:
    namespace['into'] = Dispatcher('into')
into = namespace['into']


@into.register(type, object)
def into_type(a, b, **kwargs):
    with ignoring(NotImplementedError):
        if 'dshape' not in kwargs:
            kwargs['dshape'] = discover(b)
    return convert(a, b, **kwargs)


@into.register(object, object)
def into_object(target, source, **kwargs):
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
        source = resource(source, **kwargs)
    with ignoring(NotImplementedError):
        if 'dshape' not in kwargs:
            kwargs['dshape'] = discover(source)
    return append(target, source, **kwargs)


@into.register(str, object)
def into_string(uri, b, **kwargs):
    ds = kwargs.pop('dshape', None)
    if not ds:
        ds = discover(b)
    if isdimension(ds[0]):
        resource_ds = 0 * ds.subshape[0]
    else:
        resource_ds = ds

    a = resource(uri, dshape=resource_ds, expected_dshape=ds, **kwargs)
    return into(a, b, dshape=ds, **kwargs)


@into.register((type, str), str)
def into_string_string(a, b, **kwargs):
    r = resource(b, **kwargs)
    return into(a, r, **kwargs)


@into.register(object)
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
