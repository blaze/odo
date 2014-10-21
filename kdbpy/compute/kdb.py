"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division
from builtins import str
from builtins import zip

from past.builtins import basestring

import numbers
import pprint
import keyword
import re

from collections import OrderedDict
from io import StringIO
from functools import total_ordering

from blaze.dispatch import dispatch
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, Reduction
from blaze.expr import count
from toolz.curried import map
from toolz import identity


def isidentifier(s):
    return re.match(r'([a-zA-Z_]\w*)', s) is not None and ' ' not in s


class QDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        super(QDict, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '%s!%s' % (QList(*self.keys()), QList(*self.values()))

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.keys() == other.keys() and
                self.values() == other.values())

    def __ne__(self, other):
        return not self == other


@total_ordering
class QCategorical(object):
    def __init__(self, s):
        assert isinstance(s, (basestring, QSymbol, QString))
        self.s = getattr(s, 's', s)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return self.s == other.s

    def __lt__(self, other):
        return self.s < other.s


class QString(QCategorical):
    def __init__(self, s):
        super(QString, self).__init__(s)

    def __repr__(self):
        return '"%s"' % self.s


class QSymbol(QCategorical):
    def __init__(self, s):
        super(QSymbol, self).__init__(s)

    def __repr__(self):
        s = self.s
        if not isidentifier(s) and not keyword.iskeyword(s):
            return '`$%s' % QString(s)
        return '`' + s


class QOperator(QSymbol):
    def __repr__(self):
        return self.s


@total_ordering
class QList(object):
    def __init__(self, *items):
        self.items = items

    def __repr__(self):
        if len(self) == 1:
            return '(enlist[%s])' % self[0]
        return '(%s)' % '; '.join(map(str, self.items))

    def __getitem__(self, key):
        result = self.items[key]
        if isinstance(key, slice):
            return QList(*result)
        return result

    def __add__(self, other):
        return QList(*(self.items + other.items))

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __eq__(self, other):
        return type(self) == type(other) and self.items == other.items

    def __lt__(self, other):
        return type(self) == type(other) and self.items < other.items


class Q(object):
    __slots__ = 't', 'q'

    def __init__(self, t, q):
        self.t = t
        self.q = q

    def __repr__(self):
        stream = StringIO()
        pprint.pprint(self.q, stream=stream)
        return ('{0.__class__.__name__}'
                '(t={0.t}, q={1!s})').format(self, stream.getvalue())


@dispatch(basestring, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=QString(str(expr)))


@dispatch(Projection, Q)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs).q
    fields = list(map(QSymbol, expr.fields))
    q = QList('?', child, QList(), QBool(False),
              QDict(list(zip(fields, fields))))
    return Q(t=expr, q=q)


@dispatch(Symbol, Q)
def compute_up(expr, data, **kwargs):
    if isinstance(data.t, Symbol) and expr._name in data.t.fields:
        return Q(t=expr, q=QSymbol('%s.%s' % (data.t, expr._name)))
    return Q(t=expr, q=QSymbol(expr._name))


binops = {
    '!=': QOperator('<>'),
    '/': QOperator('%'),
    '%': QOperator('mod'),
    '**': QOperator('xexp'),
    '==': QOperator('=')
}


unops = {
    'USub': QOperator('-:'),
    '~': QOperator('~:')
}


@dispatch(numbers.Number, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=str(expr))


@dispatch(BinOp, Q)
def compute_up(expr, data, **kwargs):
    op = binops.get(expr.symbol, expr.symbol)
    lhs = compute_up(expr.lhs, data, **kwargs).q
    rhs = compute_up(expr.rhs, data, **kwargs).q
    return Q(t=expr, q=QList(op, lhs, rhs))


@dispatch(UnaryOp, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=QList(unops.get(expr.symbol, expr.symbol),
                             compute_up(expr._child, data, **kwargs).q))


@dispatch(Field, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q='%s.%s' % (compute_up(expr._child, data, **kwargs).q,
                                  expr._name))


@dispatch(Broadcast, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=compute_up(expr._expr, data, **kwargs).q)


@dispatch(Selection, Q)
def compute_up(expr, data, **kwargs):
    predicate = compute_up(expr.predicate, data, **kwargs).q
    return Q(t=expr, q=QList('?', compute_up(expr._child, data, **kwargs).q,
                             QList(QList(predicate)), QBool(False), ()))


reductions = {'mean': 'avg',
              'std': 'dev'}


@dispatch(count, Q)
def compute_up(expr, data, **kwargs):
    target = compute_up(expr._child, data, **kwargs).q
    return Q(t=expr, q='count ({0}) where not null ({0})'.format(target))


@dispatch(Reduction, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=QList(reductions.get(expr.symbol, expr.symbol),
                             compute_up(expr._child, data, **kwargs).q))


def call_q_client(expr):
    """Send an expression to the KDB interpreter.

    Parameters
    ----------
    expr : str
        A valid Q expression
    """
    return expr


server = identity


@total_ordering
class QBool(object):
    def __init__(self, value):
        assert value is True or value is False
        self.value = value

    def __repr__(self):
        return '%ib' % self.value

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    def __lt__(self, other):
        return type(self) == type(other) and self.value < other.value


@dispatch(Expr, Q, dict)
def post_compute(expr, data, _):
    return server(data.q)
