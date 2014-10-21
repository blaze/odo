"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division
from builtins import str
from builtins import zip

from past.builtins import basestring

import numbers
import pprint

from . import q

from io import StringIO

from blaze.dispatch import dispatch
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, Reduction
from blaze.expr import count
from toolz.curried import map
from toolz import identity


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
    return Q(t=expr, q=q.String(str(expr)))


@dispatch(Projection, Q)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs).q
    fields = list(map(q.Symbol, expr.fields))
    qexpr = q.List('?', child, q.List(), q.Bool(False),
                   q.Dict(list(zip(fields, fields))))
    return Q(t=expr, q=qexpr)


@dispatch(Symbol, Q)
def compute_up(expr, data, **kwargs):
    if isinstance(data.t, Symbol) and expr._name in data.t.fields:
        return Q(t=expr, q=q.Symbol('%s.%s' % (data.t, expr._name)))
    return Q(t=expr, q=q.Symbol(expr._name))


binops = {
    '!=': q.Operator('<>'),
    '/': q.Operator('%'),
    '%': q.Operator('mod'),
    '**': q.Operator('xexp'),
    '==': q.Operator('=')
}


unops = {
    'USub': q.Operator('-:'),
    '~': q.Operator('~:')
}


@dispatch(numbers.Number, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=str(expr))


@dispatch(BinOp, Q)
def compute_up(expr, data, **kwargs):
    op = binops.get(expr.symbol, expr.symbol)
    lhs = compute_up(expr.lhs, data, **kwargs).q
    rhs = compute_up(expr.rhs, data, **kwargs).q
    return Q(t=expr, q=q.List(op, lhs, rhs))


@dispatch(UnaryOp, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=q.List(unops.get(expr.symbol, expr.symbol),
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
    return Q(t=expr, q=q.List('?', compute_up(expr._child, data, **kwargs).q,
                              q.List(q.List(predicate)), q.Bool(False), ()))


reductions = {'mean': 'avg',
              'std': 'dev'}


@dispatch(count, Q)
def compute_up(expr, data, **kwargs):
    target = compute_up(expr._child, data, **kwargs).q
    return Q(t=expr, q='count ({0}) where not null ({0})'.format(target))


@dispatch(Reduction, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=q.List(reductions.get(expr.symbol, expr.symbol),
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


@dispatch(Expr, Q, dict)
def post_compute(expr, data, _):
    return server(data.q)
