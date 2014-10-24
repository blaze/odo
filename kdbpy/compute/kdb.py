"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division

import numbers

from . import q
from ..kdb import KQ, Credentials

import qpython.qcollection

import pandas as pd
import sqlalchemy as sa

from blaze import compute
from blaze.dispatch import dispatch
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, Reduction, By, Join, Head
from blaze.expr import count

from datashape import DataShape, Var, Record

from toolz.curried import map
from toolz.compatibility import zip
from toolz import identity, first


class QTable(object):
    def __init__(self, uri):
        self.uri, self._tablename = uri.rsplit('::', 1)
        self.params = params = sa.engine.url.make_url(self.uri)
        self.engine = KQ(Credentials(username=params.username,
                                     password=params.password, host=params.host,
                                     port=params.port))
        self.dshape = tables(self.engine)[self.tablename].dshape

    @property
    def tablename(self):
        return self._tablename

    def __repr__(self):
        return ('{0.__class__.__name__}(tablename={0.tablename!r}, '
                'dshape={0.dshape!r})'.format(self))


qtypes = {'b': 'bool',
          'x': 'int8',
          'h': 'int16',
          'i': 'int32',
          'j': 'int64',
          'e': 'float32',
          'f': 'float64',
          'c': 'int8',  # q char
          's': 'string',  # q symbol
          'm': 'date',  # q month
          'd': 'date',
          'z': 'datetime',
          'u': 'time',  # q minute
          'v': 'time',  # q second
          't': 'time'}


@dispatch(basestring, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.Symbol(expr)


@dispatch(Projection, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    fields = list(map(q.Symbol, expr.fields))
    qexpr = q.List('?', child, q.List(), q.Bool(False),
                   q.Dict(list(zip(fields, fields))))
    return qexpr


def tables(kdb):
    names = kdb.eval('tables `.')
    metadata = kdb.eval('meta each tables `.')

    # t is the type column in Q
    syms = []
    for name, metatable in zip(names, metadata):
        types = metatable.t.tolist()
        ds = DataShape(Var(), Record(list(zip(names,
                                              [qtypes[t] for t in types]))))
        syms.append((name, Symbol(name, ds)))
    return dict(syms)


@dispatch(Symbol, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.Symbol(expr._name)


binops = {
    '!=': q.Atom('<>'),
    '/': q.Atom('%'),
    '%': q.Atom('mod'),
    '**': q.Atom('xexp'),
    '==': q.Atom('=')
}


unops = {
    'USub': q.Atom('-:'),
    '~': q.Atom('~:')
}


@dispatch(numbers.Number, q.Expr)
def compute_up(expr, data, **kwargs):
    return expr


def get_wrapper(expr, types=(basestring,)):
    return q.List if isinstance(expr, types) else identity


@dispatch(BinOp, q.Expr)
def compute_up(expr, data, **kwargs):
    op = binops.get(expr.symbol, expr.symbol)
    lwrapper = get_wrapper(expr.lhs)
    rwrapper = get_wrapper(expr.rhs)
    lhs = lwrapper(compute_up(expr.lhs, data, **kwargs))
    rhs = rwrapper(compute_up(expr.rhs, data, **kwargs))
    return q.List(op, lhs, rhs)


@dispatch(UnaryOp, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List(unops.get(expr.symbol, expr.symbol), compute_up(expr._child,
                                                                  data,
                                                                  **kwargs))


@dispatch(Field, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.Symbol('%s.%s' % (compute_up(expr._child, data, **kwargs),
                               expr._name))


@dispatch(Broadcast, q.Expr)
def compute_up(expr, data, **kwargs):
    return compute_up(expr._expr, data, **kwargs)


@dispatch(Selection, q.Expr)
def compute_up(expr, data, **kwargs):
    predicate = compute_up(expr.predicate, data, **kwargs)
    return q.List('?', compute_up(expr._child, data, **kwargs),
                  q.List(q.List(predicate)), q.Bool(), ())


reductions = {'mean': 'avg',
              'std': 'dev'}


@dispatch(count, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    return q.List('#:', child)


@dispatch(Reduction, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List(reductions.get(expr.symbol, expr.symbol),
                  compute_up(expr._child, data, **kwargs))


@dispatch(Join, q.Expr)
def compute_up(expr, data, **kwargs):
    return data


@dispatch(By, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    grouper = compute_up(expr.grouper, data, **kwargs)
    grouper = q.Dict([(q.Symbol(expr.grouper._name), grouper)])
    reducer = compute_up(expr.apply, data, **kwargs)
    reducer = q.Dict([(q.Symbol(expr.apply._name), reducer)])
    qexpr = q.List('?', child, (), grouper, reducer)
    return qexpr


@dispatch(Head, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    return q.List('#', min(expr.n, compute(expr._child.count(), data)), child)


@dispatch(Expr, q.Expr, dict)
def post_compute(expr, data, scope):
    assert len(scope) == 1
    return first(scope.values()).engine.eval('eval [%s]' % data)


@dispatch(Expr, QTable)
def compute_up(expr, data, **kwargs):
    return compute_up(expr, q.Symbol(data.tablename), **kwargs)


@dispatch(pd.DataFrame, qpython.qcollection.QKeyedTable)
def into(df, tb, **kwargs):
    keys = tb.keys
    keynames = keys.dtype.names
    index = pd.MultiIndex.from_arrays([keys[name] for name in keynames],
                                      names=keynames)
    return pd.DataFrame.from_records(tb.values, index=index)
