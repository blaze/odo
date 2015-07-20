from __future__ import absolute_import, division, print_function

import collections
import importlib
import inspect
import re

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

convert_hop = collections.namedtuple(
    'ConvertHop', ['source', 'target', 'func', 'mod', 'code'])

class Path(object):
    """Contains information about the conversion path that odo will take between
    two types.

    Two types of information are contained - information on an append operation
    and information on a convert operation. Append function calls will frequently
    contain convert calls as well, so both the append and convert attributes may
    have values.

    Positional arguments:
    source -- The object odo is converting from.
    target -- The object or type odo is converting to."""

    def __init__(self, source, target):
        self.source = source
        self.target = target
        self.append_sig = None
        self.append_func = None
        self.append_mod = None
        self.append_code = None
        self.convert_sig = None
        self.convert_path = []

    def add_append(self, func):
        """Add information about an append function call to the Path object.

        Positional argument:
        func -- An function that MultipleDispatch returns for a given append
            signature."""

        self.append_func = func
        self.append_mod = inspect.getmodule(func)
        self.append_code = inspect.getsource(func)

    def add_convert(self, path):
        """Add information about a convert path to the Path object.

        Positional argument:
        path -- A list of tuples containing source type, target type, and function
            that NetworkX returns for a given convert signature."""

        for (src, tgt, func) in path:
            hop = convert_hop(src, tgt, func, inspect.getmodule(func),
                              inspect.getsource(func))
            self.convert_path.append(hop)

    def simulate_convert(self, a, b):
        """Query networkx to see what the shortest path is from type b to type a."""

        if not isinstance(a, type):
            a = type(a)
        if not isinstance(b, type):
            b = type(b)

        convert_path = convert.path(b, a)
        self.add_convert(convert_path)

    def __str__(self):
        if isinstance(self.source, type):
            source = self.source.__name__ + ' type'
        else:
            source = type(self.source).__name__ + ' object'
        if isinstance(self.target, type):
            target = self.target.__name__ + ' type'
        else:
            target = type(self.target).__name__ + ' object'

        outstr = 'Odo path simulation from {src} to {tgt}\n'.format(
            src=source, tgt=target)
        outstr += '------------------------------------------------------------\n'
        if self.append_sig:
            outstr += 'Append will be called from {src} to {tgt}\n'.format(
                src=self.append_sig[1].__name__,
                tgt=self.append_sig[0].__name__)
            outstr += 'Append will dispatch function {func} from {mod}\n'.format(
                func=self.append_func.__name__, mod=self.append_mod.__name__)
            outstr += 'Function {func} code follows:\n'.format(
                func=self.append_func.__name__)

            for line in self.append_code.split('\n'):
                outstr += '    ' + line + '\n'

        if self.convert_sig:
            if isinstance(self.convert_sig[0], basestring):
                target = self.convert_sig[0]
            else:
                target = self.convert_sig[0].__name__
            outstr += 'Convert will be called from {src} to {tgt}\n'.format(
                src=self.convert_sig[1].__name__, tgt=target)

        if self.convert_path:
            for (n, hop) in enumerate(self.convert_path, start=1):
                outstr += 'Conversion hop {n}: {src} to {tgt}\n'.format(
                    n=n, src=hop.source.__name__, tgt=hop.target.__name__)
                outstr += 'Hop dispatches function {func} from {mod}\n'.format(
                    func=hop.func.__name__, mod=hop.mod.__name__)
                outstr += 'Function {func} code follows:\n'.format(
                    func=hop.func.__name__)

                for line in hop.code.split('\n'):
                    outstr += '    ' + line + '\n'
        elif self.convert_sig:
           outstr += ('Convert pathing could not be determined (maybe {} is not '
                    'a type in a reachable namespace?)'.format(self.convert_sig[0]))

        return outstr

    # Eventually make __repr__ more Pythonic
    __repr__ = __str__


@into.register(type, object)
def into_type(a, b, dshape=None, simulate=False, **kwargs):
    if simulate:
        path = Path(b, a)
        path.convert_sig = (a, type(b))
        path.simulate_convert(a, b)
        return path

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
        path = Path(source, target)
        path.append_sig = (type(target), type(source))

        path.add_append(append.dispatch(type(target), type(source)))

        # Look for a convert call inside the append function code
        match = re.search(r'convert\((\w+),', path.append_code, flags=re.I)
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

                path.convert_sig = (convert_type, type(source))
                path.simulate_convert(convert_type, source)
            except:
                path.convert_sig = (convert_str, type(source))

        return path

    if isinstance(source, (str, unicode)):
        source = resource(source, dshape=dshape, **kwargs)
    with ignoring(NotImplementedError):
        if dshape is None:
            dshape = discover(source)
    return append(target, source, dshape=dshape, **kwargs)


@into.register((str, unicode), object)
def into_string(uri, b, dshape=None, simulate=False, **kwargs):
    if dshape is None:
        dshape = discover(b)

    resource_ds = 0 * dshape.subshape[0] if isdimension(dshape[0]) else dshape

    a = resource(uri, dshape=resource_ds, expected_dshape=dshape, **kwargs)

    return into(a, b, dshape=dshape, simulate=simulate, **kwargs)


@into.register((type, (str, unicode)), (str, unicode))
def into_string_string(a, b, simulate=False, **kwargs):
    return into(a, resource(b, **kwargs), simulate=simulate, **kwargs)


@into.register(object)
def into_curried(o, **kwargs1):
    def curried_into(other, **kwargs2):
        return into(o, other, **merge(kwargs2, kwargs1))
    return curried_into
