from __future__ import absolute_import, division, print_function

import inspect
import re
import importlib

from toolz import merge
from multipledispatch import Dispatcher
from multipledispatch.dispatcher import str_signature
from .convert import convert
from .append import append
from .resource import resource
from .utils import ignoring
from datashape import discover
from datashape.dispatch import namespace
from datashape.predicates import isdimension
from .compatibility import unicode


if 'into' not in namespace:
    namespace['into'] = Dispatcher('into')
into = namespace['into']

def simulate_convert(a, b):
    if not isinstance(a, type):
        a = type(a)
    if not isinstance(b, type):
        b = type(b)
    print('convert will be called with arguments: ({})'.format(
        str_signature((a, b))))

    convert_path = convert.path(b, a)
    for (n, (src, tgt, func)) in enumerate(convert_path, start=1):
        print('Conversion hop {n}: {src} to {tgt} by function {func}:\n'
            '{func_txt}'.format(n=n, src=src.__name__, tgt=tgt.__name__,
            func=func.__name__, func_txt=inspect.getsource(func)))

@into.register(type, object)
def into_type(a, b, dshape=None, simulate=False, **kwargs):
    if simulate:
        print('In into.into_type')
        simulate_convert(a, b)
        return

    with ignoring(NotImplementedError):
        if dshape is None:
            dshape = discover(b)
    return convert(a, b, dshape=dshape, simulate=simulate, **kwargs)


@into.register(object, object)
def into_object(target, source, dshape=None, simulate=False, **kwargs):
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
    if simulate:
        print('In into.into_object')
        print('append will be called with arguments: ({})'.format(
            str_signature((type(target), type(source)))))

        dispatched_func = append.dispatch(type(target), type(source))

        print('This will call the function: {}'.format(dispatched_func.__name__))
        print('This function looks like:')
        func_src = inspect.getsource(dispatched_func)
        print(func_src)

        match = re.search(r'convert\((\w+),', func_src, flags=re.I)
        if match:
            convert_str = match.group(1)
            try:
                # Convert object name string to actual object type, if possible
                try:
                    module = importlib.import_module('__builtin__')
                    convert_type = getattr(module, convert_str)
                except AttributeError:
                    module, convert_str = convert_str.rsplit(".", 1)
                    module = importlib.import_module(module)
                    convert_type = getattr(module, convert_str)

                simulate_convert(convert_type, source)
            except:
                print('convert will be called with arguments: ({0}, {1})'.format(
                    convert_str, type(source).__name__))
                print('Convert pathing could not be determined (maybe {} is not '
                    'a type in a reachable namespace?)'.format(convert_str))
                raise

        return

    if isinstance(source, (str, unicode)):
        source = resource(source, dshape=dshape, **kwargs)
    with ignoring(NotImplementedError):
        if dshape is None:
            dshape = discover(source)
    return append(target, source, dshape=dshape, **kwargs)


@into.register((str, unicode), object)
def into_string(uri, b, dshape=None, simulate=False, **kwargs):
    if simulate:
        print('In into.into_string')

    if dshape is None:
        dshape = discover(b)

    resource_ds = 0 * dshape.subshape[0] if isdimension(dshape[0]) else dshape

    a = resource(uri, dshape=resource_ds, expected_dshape=dshape, **kwargs)

    return into(a, b, dshape=dshape, simulate=simulate, **kwargs)


@into.register((type, (str, unicode)), (str, unicode))
def into_string_string(a, b, simulate=False, **kwargs):
    if simulate:
        print('In into.into_string_string')

    return into(a, resource(b, **kwargs), simulate=simulate, **kwargs)


@into.register(object)
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
