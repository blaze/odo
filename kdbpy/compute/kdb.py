"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division

import numbers
import pprint
import keyword
import re

from collections import Iterable, OrderedDict
from io import StringIO

from blaze.dispatch import dispatch
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, By, Reduction
from blaze.expr import count
from future.builtins import str
from toolz.curried import map
from toolz import first


Leaf = Field, Symbol, numbers.Number


ID_REGEX = re.compile(r'[a-zA-Z_]\w*')


def isidentifier(s):
    return ID_REGEX.match(s) is not None and not keyword.iskeyword(s)


class QDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        super(QDict, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '%s!%s' % (QList(*self.keys()), QList(*self.values()))


class QSymbol(object):
    def __init__(self, s):
        assert isinstance(s, str), 'input must be an instance of str'
        self.s = s

    def __repr__(self):
        if not isidentifier(self.s):
            return '`$"%s"' % self.s
        return '`%s' % self.s

    def __hash__(self):
        return hash(self.s)


class QList(object):
    def __init__(self, *items):
        super(QList, self).__init__()
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


@dispatch(str, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=QSymbol(str(expr)))


def _deep_join(qexpr):
    for el in qexpr:
        if isinstance(el, str) or not isinstance(el, Iterable):
            yield str(el)
        else:
            yield '(%s)' % '; '.join(_deep_join(el))


def deep_join(qexpr, char='; '):
    return '(%s)' % char.join(_deep_join(qexpr))


@dispatch(Projection, Q)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    fields = list(map(QSymbol, expr.fields))
    q = QList('?', child.q, '0b', QDict(zip(fields, fields)))
    return Q(t=expr, q=q)


@dispatch(Symbol, Q)
def compute_up(expr, data, **kwargs):
    if isinstance(data.t, Symbol) and expr._name in data.t.fields:
        return Q(t=expr, q=QSymbol('%s.%s' % (data.t, expr._name)))
    return Q(t=expr, q=QSymbol(expr._name))


binops = {
    '&': 'and',
    '|': 'or',
    '!=': '<>',
    '/': '%',
    '%': 'mod',
    '**': 'xexp',
    '==': '='
}


unops = {
    'USub': '-:',
    '~': '~:'
}


@dispatch(numbers.Number, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=str(expr))


@dispatch(BinOp, Q)
def compute_up(expr, data, **kwargs):
    op = binops.get(expr.symbol, expr.symbol)
    lhs = compute_up(expr.lhs, data, **kwargs).q
    rhs = compute_up(expr.rhs, data, **kwargs).q
    return Q(t=expr, q='(%s)' % '; '.join(map(str, (op, lhs, rhs))))


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
    return Q(t=expr, q=compute_up(expr.expr, data, **kwargs).q)


@dispatch(Selection, Q)
def compute_up(expr, data, **kwargs):
    predicate = compute_up(expr.predicate, data, **kwargs).q
    if isinstance(expr._child, Leaf):
        q = QList(compute_up(expr._child, data, **kwargs).q,
                  QList(QList(predicate)), '0b', ())
        return Q(t=expr, q=q)
    q = QList('?', compute_up(expr._child, data, **kwargs).q,
              QList(QList(predicate)), '0b', ())
    return Q(t=expr, q=q)


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


@dispatch(By, Q)
def compute_up(expr, data, **kwargs):
    grouper = compute_up(expr.grouper, data, **kwargs).q
    reduction = compute_up(expr.apply, data, **kwargs).q
    child = compute_up(expr.grouper._child, data, **kwargs).q
    return Q(t=expr, q='select (%s) by (%s) from (%s)' % (reduction, grouper,
                                                          child))


def call_q_client(expr):
    """Send an expression to the KDB interpreter.

    Parameters
    ----------
    expr : str
        A valid Q expression
    """
    return expr


@dispatch(Expr, Q, dict)
def post_compute(expr, data, _):
    return call_q_client(str(data.q))
