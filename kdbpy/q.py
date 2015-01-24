import re
import keyword
from itertools import chain
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
    def __init__(self, items):
        super(Dict, self).__init__(items)

    def __repr__(self):
        return '%s!%s' % (List(*self.keys()), List(*self.values()))


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
    def __init__(self, *args, **kwargs):
        """
        Examples
        --------
        >>> from kdbpy import q
        >>> t = q.Symbol('t', 's', 'a')
        >>> t
        `t.s.a
        """
        super(Symbol, self).__init__(args[0], **kwargs)
        self.fields = args[1:]
        self.str = '.'.join(chain([self.s], self.fields))

    def __getitem__(self, name):
        """
        Examples
        --------
        >>> from kdbpy import q
        >>> t = q.Symbol('t')
        >>> t['s']['a']
        `t.s.a
        """
        return type(self)(*list(chain([self.s], self.fields, [name])))

    def __repr__(self):
        joined = self.str
        if not isidentifier(joined) and not keyword.iskeyword(joined):
            return '`$"%s"' % joined
        return '`' + joined


class List(object):
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
    is_partitioned = False
    is_splayed = False

    def __init__(self, value=False):
        self.value = bool(value)

    def __repr__(self):
        return '%ib' % self.value

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    def __ne__(self, other):
        return not (self == other)


def binop(op):
    return lambda x, y: List(op, x, y)


def unop(op):
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


def xor(x, y):
    return and_(or_(x, y), not_(and_(x, y)))


def floordiv(x, y):
    return floor(div(x, y))


neg = unop('-:')
null = unop('^:')
not_ = unop('~:')
floor = unop('_:')
ceil = unop('-_-:')
count = unop('#:')
til = unop('til')
distinct = unop('?:')
typeof = unop('type')
istable = unop('.Q.qt')


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
}


unops = {'-': neg,
         '~': not_,
         'floor': floor,
         'ceil': ceil,
         # reductions
         'sum': unop('sum'),
         'mean': unop('avg'),
         'std': unop('dev'),
         'var': unop('var'),
         'min': unop('min'),
         'max': unop('max'),
         'count': count,
         'nelements': count,
         'nunique': unop('.kdbpy.nunique')}


def symlist(*args):
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
    return eq(typeof(x), QDICTIONARY)


def cast(typ):
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
