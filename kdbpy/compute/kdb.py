"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division

import numbers
import pprint

from . import q

from io import StringIO
import qpython.qcollection

import pandas as pd

from blaze.dispatch import dispatch
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, Reduction, By
from blaze.expr import count
from toolz.curried import map
from toolz.compatibility import zip
from toolz import identity


class Q(object):
    __slots__ = 't', 'q', 'kdb'

    def __init__(self, t, q, kdb):
        self.t = t
        self.q = q
        self.kdb = kdb

    def __repr__(self):
        stream = StringIO()
        pprint.pprint(self.q, stream=stream)
        return ('{0.__class__.__name__}'
                '(t={0.t}, q={1!s})').format(self, stream.getvalue())

    def eval(self, *args, **kwargs):
        return self.kdb.kdb.eval(*args, **kwargs)


@dispatch(basestring, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=q.Symbol(expr), kdb=data.kdb)


@dispatch(Projection, Q)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs).q
    fields = list(map(q.Symbol, expr.fields))
    qexpr = q.List('?', child, q.List(), q.Bool(False),
                   q.Dict(list(zip(fields, fields))))
    return Q(t=expr, q=qexpr, kdb=data.kdb)


@dispatch(Symbol, Q)
def compute_up(expr, data, **kwargs):
    if isinstance(data.t, Symbol) and expr._name in data.t.fields:
        return Q(t=expr, q=q.Symbol('%s.%s' % (data.t, expr._name)),
                 kdb=data.kdb)
    return Q(t=expr, q=q.Symbol(expr._name), kdb=data.kdb)


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
    return Q(t=expr, q=str(expr), kdb=data.kdb)


@dispatch(BinOp, Q)
def compute_up(expr, data, **kwargs):
    op = binops.get(expr.symbol, expr.symbol)
    lwrapper = q.List if isinstance(expr.lhs, basestring) else identity
    rwrapper = q.List if isinstance(expr.rhs, basestring) else identity
    lhs = lwrapper(compute_up(expr.lhs, data, **kwargs).q)
    rhs = rwrapper(compute_up(expr.rhs, data, **kwargs).q)
    qexpr = q.List(op, lhs, rhs)
    return Q(t=expr, q=qexpr, kdb=data.kdb)


@dispatch(UnaryOp, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=q.List(unops.get(expr.symbol, expr.symbol),
                              compute_up(expr._child, data, **kwargs).q),
             kdb=data.kdb)


@dispatch(Field, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q='%s.%s' % (compute_up(expr._child, data, **kwargs).q,
                                  expr._name), kdb=data.kdb)


@dispatch(Broadcast, Q)
def compute_up(expr, data, **kwargs):
    return Q(t=expr, q=compute_up(expr._expr, data, **kwargs).q, kdb=data.kdb)


@dispatch(Selection, Q)
def compute_up(expr, data, **kwargs):
    predicate = compute_up(expr.predicate, data, **kwargs).q
    return Q(t=expr, q=q.List('?', compute_up(expr._child, data, **kwargs).q,
                              q.List(q.List(predicate)), q.Bool(False), ()),
             kdb=data.kdb)


reductions = {'mean': 'avg',
              'std': 'dev'}


@dispatch(count, Q)
def compute_up(expr, data, **kwargs):
    target = compute_up(expr._child, data, **kwargs).q
    return Q(t=expr, q='count ({0}) where not null ({0})'.format(target),
             kdb=data.kdb)


@dispatch(Reduction, Q)
def compute_up(expr, data, **kwargs):
    qexpr = q.List(reductions.get(expr.symbol, expr.symbol),
                   compute_up(expr._child, data, **kwargs).q)
    return Q(t=expr, q=qexpr, kdb=data.kdb)


@dispatch(By, Q)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs).q
    grouper = compute_up(expr.grouper, data, **kwargs).q
    grouper = q.Dict([(q.Symbol(expr.grouper._name), grouper)])
    reducer = compute_up(expr.apply, data, **kwargs).q
    reducer = q.Dict([(q.Symbol(expr.apply._name), reducer)])
    qexpr = q.List('?', child, (), grouper, reducer)
    return Q(t=expr, q=qexpr, kdb=data.kdb)


@dispatch(Expr, Q, dict)
def post_compute(expr, data, _):
    return data.eval('eval[%s]' % data.q)


@dispatch(pd.DataFrame, qpython.qcollection.QKeyedTable)
def into(df, tb, **kwargs):
    keys = tb.keys
    keynames = keys.dtype.names
    index = pd.MultiIndex.from_arrays([keys[name] for name in keynames],
                                      names=keynames)
    return pd.DataFrame.from_records(tb.values, index=index)
