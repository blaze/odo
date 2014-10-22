import re
import keyword

from collections import OrderedDict
from functools import total_ordering


def isidentifier(s):
    return re.match(r'([a-zA-Z_]\w*)', s) is not None and ' ' not in s


class Expr(object):
    pass


class Dict(OrderedDict, Expr):
    def __init__(self, *args, **kwargs):
        super(Dict, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '%s!%s' % (List(*self.keys()), List(*self.values()))

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.keys() == other.keys() and
                self.values() == other.values())

    def __ne__(self, other):
        return not self == other


@total_ordering
class Categorical(Expr):
    def __init__(self, s):
        assert isinstance(s, (basestring, Symbol, String))
        self.s = getattr(s, 's', s)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return self.s == other.s

    def __lt__(self, other):
        return self.s < other.s


class String(Categorical):
    def __init__(self, s):
        super(String, self).__init__(str(s))

    def __repr__(self):
        return '"%s"' % self.s


class Symbol(Categorical):
    def __init__(self, s):
        super(Symbol, self).__init__(s)

    def __repr__(self):
        s = self.s
        if not isidentifier(s) and not keyword.iskeyword(s):
            return '`$%s' % String(s)
        return '`' + s


class Operator(Symbol):
    def __init__(self, *args, **kwargs):
        super(Operator, self).__init__(*args, **kwargs)

    def __repr__(self):
        return self.s


@total_ordering
class List(Expr):
    def __init__(self, *items):
        self.items = items

    def __repr__(self):
        if len(self) == 1:
            return '(enlist[%s])' % self[0]
        return '(%s)' % '; '.join(map(str, self.items))

    def __getitem__(self, key):
        result = self.items[key]
        if isinstance(key, slice):
            return List(*result)
        return result

    def __add__(self, other):
        return List(*(self.items + other.items))

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __eq__(self, other):
        return type(self) == type(other) and self.items == other.items

    def __lt__(self, other):
        return type(self) == type(other) and self.items < other.items


@total_ordering
class Bool(Expr):
    def __init__(self, value=False):
        self.value = bool(value)

    def __repr__(self):
        return '%ib' % self.value

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    def __lt__(self, other):
        return type(self) == type(other) and self.value < other.value
