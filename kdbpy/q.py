from __future__ import print_function, division, absolute_import

import re
import keyword
from itertools import chain, starmap
from toolz import compose
from toolz.compatibility import zip, map

try:
    import builtins
except ImportError:  # pragma: no cover
    import __builtin__ as builtins
from qpython.qcollection import QDICTIONARY

from collections import OrderedDict


def isidentifier(s, rx=re.compile(r'[a-zA-Z_]\w*')):
    return rx.match(s) is not None and ' ' not in s


class Expr(object):
    def __init__(self, is_splayed=False, is_partitioned=False):
        self.is_splayed = is_splayed
        self.is_partitioned = is_partitioned


class Dict(OrderedDict):
    """q dictionary. Unlike Python q's dicts are ordered, so we must construct
    them with tuples.

    Examples
    --------
    >>> items = [(Symbol('a'), 1), (Symbol('b'), 2)]
    >>> Dict(items)
    (`a; `b)!(1; 2)
    """
    def __repr__(self):
        return '%s!%s' % tuple(starmap(List, zip(*self.items())))


class Atom(Expr):
    def __init__(self, s, **kwargs):
        super(Atom, self).__init__(**kwargs)
        assert isinstance(s, (basestring, Atom, String))
        self.s = getattr(s, 's', s)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return type(self) == type(other) and self.s == other.s

    def __repr__(self):
        return self.s


class String(Atom):
    def __init__(self, s, **kwargs):
        super(String, self).__init__(str(s), **kwargs)

    def __repr__(self):
        return '"%s"' % self.s


class Symbol(Atom):
    """Represents a q symbol

    Examples
    --------
    >>> from kdbpy import q
    >>> t = q.Symbol('t', 's', 'a')
    >>> t
    `t.s.a
    >>> # getitem syntax
    >>> from kdbpy import q
    >>> t = q.Symbol('t')
    >>> t['s']['a']
    `t.s.a
    >>> Symbol('2')
    `$"2"
    """
    def __init__(self, *args, **kwargs):
        super(Symbol, self).__init__(args[0], **kwargs)
        self.fields = args[1:]
        self.str = '.'.join(chain([self.s], self.fields))

    def __getitem__(self, name):
        return type(self)(*list(chain([self.s], self.fields, [name])))

    def __repr__(self):
        joined = self.str
        if not isidentifier(joined) and not keyword.iskeyword(joined):
            return '`$"%s"' % joined
        return '`' + joined

    def __eq__(self, other):
        return type(self) == type(other) and self.str == other.str


class List(object):
    """List of q objects.

    This is the main structure for describing q code

    Examples
    --------
    >>> qlist = List(Symbol('a'), 1, List(2))
    >>> qlist
    (`a; 1; (,:[2]))
    """
    is_partitioned = False
    is_splayed = False

    def __init__(self, *items):
        self.items = list(items)

    def __repr__(self):
        if len(self) == 1:
            return '(,:[%s])' % self[0]
        return '(%s)' % '; '.join(map(str, self.items))

    def __getitem__(self, key):
        result = self.items[key]
        if isinstance(key, builtins.slice):
            return List(*result)
        return result

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __eq__(self, other):
        return type(self) == type(other) and self.items == other.items

    def __add__(self, other):
        return List(*list(chain(self.items, other.items)))

    def append(self, other):
        return self + type(self)(other)


class Bool(object):
    """Class to print q booleans from Python booleans

    Examples
    --------
    >>> Bool(True)
    1b
    >>> Bool(False)
    0b
    >>> Bool()
    0b
    """
    is_partitioned = False
    is_splayed = False

    def __init__(self, value=False):
        self.value = bool(value)

    def __repr__(self):
        return '%db' % self.value

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    def __ne__(self, other):
        return not (self == other)


def binop(op):
    """Binary operator generator

    Examples
    --------
    >>> equals = binop('=')
    >>> equals(1, 2)
    (=; 1; 2)
    """
    return lambda x, y: List(op, x, y)


def unop(op):
    """Unary operator generator

    Examples
    --------
    >>> sin = unop('sin')
    >>> sin(3.14159)
    (sin; 3.14159)
    """
    return lambda x: List(op, x)


eq = binop('=')
ne = binop('<>')
lt = binop('<')
gt = binop('>')
le = binop('<=')
ge = binop('>=')

add = binop('+')
sub = binop('-')
mul = binop('*')
div = binop('%')
pow = binop('xexp')
mod = binop('mod')

take = binop('#')
partake = binop('.Q.ind')
and_ = binop('&')
or_ = binop('|')
cor = binop('cor')
cov = binop('cov')


def xor(x, y):
    """Exclusive or, q style

    Examples
    --------
    >>> x, y = Bool(True), Bool(True)
    >>> xor(x, y)
    (&; (|; 1b; 1b); (~:; (&; 1b; 1b)))
    """
    return and_(or_(x, y), not_(and_(x, y)))


neg = unop('-:')
null = unop('^:')
not_ = unop('~:')
sqrt = unop('sqrt')
floor = unop('_:')
ceil = unop('-_-:')
count = unop('#:')
til = unop('til')
distinct = unop('?:')
typeof = unop('@:')
istable = unop('.Q.qt')
floordiv = compose(floor, div)

binops = {
    '+': add,
    '-': sub,
    '*': mul,
    '/': div,
    '**': pow,
    '%': mod,
    '//': floordiv,

    '==': eq,
    '!=': ne,
    '>': gt,
    '<': lt,
    '>=': ge,
    '<=': le,
    '|': or_,
    '&': and_,
    '^': xor,

    'cor': cor,
    'cov': cov
}


def var(x, unbiased=False, _var=unop('var')):
    y = _var(x)
    if unbiased:
        return y
    n = count(x)
    return mul(div(sub(n, 1), n), y)


def std(x, unbiased=False, _std=unop('dev')):
    y = _std(x)
    if unbiased:
        return y
    n = count(x)
    return mul(sqrt(div(sub(n, 1), n)), y)


unops = {'-': neg,
         '~': not_,
         'floor': floor,
         'ceil': ceil,
         # reductions
         'sum': unop('sum'),
         'mean': unop('avg'),
         'std': std,
         'var': var,
         'min': unop('min'),
         'max': unop('max'),
         'any': unop('any'),
         'all': unop('all'),
         'count': count,
         'nelements': count,
         'nunique': compose(count, distinct),
         'first': unop('*:'),
         'last': unop('last')
         }


first = unops['first']
last = unops['last']


def xkey(keys, table):
    """Set keys on a q table from existing columns in the table. Similar to
    ``DataFrame.set_index()``.

    Examples
    --------
    >>> t = Symbol('t')
    >>> keys = list('abc')
    >>> xkey(keys, t)
    (xkey; (,:[(`a; `b; `c)]); `t)
    >>> xkey('a', t)
    (xkey; (,:[`a]); `t)
    """
    return List('xkey', make_keys(keys), table)


def make_keys(keys):
    """Make a list of strings into a list of q symbols suitable for passing to
    join functikeyss.

    Examples
    --------
    >>> syms = list('abcdef')
    >>> make_keys(syms)
    (,:[(`a; `b; `c; `d; `e; `f)])
    >>> make_keys('a')
    (,:[`a])
    """
    return List(symlist(*keys)) if not isinstance(keys, basestring) else symlist(keys)


def symlist(*args):
    """Turn a list of strings into a list of symbols

    Examples
    --------
    >>> syms = list('abc')
    >>> symlist(*syms)
    (`a; `b; `c)
    """
    return List(*list(map(Symbol, args)))


def slice(obj, start, stop):
    return List('.kdbpy.slice', obj, start, stop)


def slice1(obj, index):
    return List('.kdbpy.slice1', obj, index)


def sort(x, key, ascending):
    sort_func = Atom('xasc' if ascending else 'xdesc')
    if isinstance(key, basestring):
        key = [key]
    return List(sort_func, List(symlist(*key)), x)


def isdict(x):
    """Test whether a value is a dictionary

    Examples
    --------
    >>> t = Symbol('t')
    >>> isdict(t)
    (=; (@:; `t); %d)
    """ % QDICTIONARY
    return eq(typeof(x), QDICTIONARY)


def cast(typ):
    """Return a function to generate casting code for a type `typ`.

    Examples
    --------
    >>> long = cast('long')
    >>> long(1)
    ($; (,:[`long]); 1)
    >>> int = cast('int')
    >>> int(1)
    ($; (,:[`int]); 1)
    """
    return lambda x: List('$', symlist(typ), x)


long = cast('long')
int = cast('int')
float = cast('float')


class select(List):

    __slots__ = 'fields',

    # None is the sentinel for the ? in the select statement
    fields = None, 'child', 'constraints', 'grouper', 'aggregates'

    def __init__(self, child, constraints=None, grouper=None, aggregates=None):
        super(select, self).__init__('?', child,
                                     constraints
                                     if constraints is not None else List(),
                                     grouper
                                     if grouper is not None else Bool(),
                                     aggregates
                                     if aggregates is not None else List())

    def __str__(self):
        return super(select, self).__str__()

    def __getattr__(self, name):
        try:
            index = self.fields.index(name)
        except ValueError:
            return object.__getattribute__(self, name)
        else:
            return self.items[index]

    def __setattr__(self, name, value):
        try:
            index = self.fields.index(name)
        except ValueError:
            object.__setattr__(self, name, value)
        else:
            self.items[index] = value


Expr = Dict, Atom, List, Bool
