"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division

import numbers

from . import q
from .qtable import QTable, tables

import qpython.qcollection

import pandas as pd

from blaze.dispatch import dispatch

import blaze as bz
from blaze import compute
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, Reduction, By, Join, Head, Sort
from blaze.expr import count

from toolz.curried import map
from toolz.compatibility import zip
from toolz import identity, first


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


@dispatch(Symbol, q.Expr)
def compute_up(expr, data, **kwargs):
    sym = q.Symbol(expr._name)
    t = first(kwargs['scope'].keys())
    if t.isidentical(expr):
        return sym
    return subs(sym, t)


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


def _subs(expr, t):
    assert isinstance(t, bz.Symbol)

    if isinstance(expr, q.Symbol):
        # TODO: how do we handle multiple tables?
        if expr.s in t.schema.measure.names:
            yield q.Symbol('%s.%s' % (t._name, expr.s))
    elif isinstance(expr, (numbers.Number, basestring, q.Atom)):
        yield expr
    elif isinstance(expr, (q.List, q.Dict)):
        for sube in expr:
            if isinstance(sube, q.List):
                yield q.List(*(x for x in _subs(sube, t)))
            elif isinstance(sube, (q.Atom, basestring, numbers.Number)):
                yield sube
            elif isinstance(sube, q.Dict):
                yield q.Dict([(_subs(x, t), _subs(y, t))
                              for x, y in sube.items])
            else:
                raise ValueError('unknown type for substitution '
                                 '{!r}'.format(type(sube).__name__))


def subs(expr, t):
    """Substitute a parent table name for fields in a broadcast expression.

    Parameters
    ----------
    expr : q.Expr, q.Atom, int, basestring
        The exression in which to substitute the parent table
    t : blaze.Symbol
        The parent table to use for substitution

    Returns
    -------
    expression : q.Expr

    Examples
    --------
    >>> from blaze import Symbol
    >>> t = Symbol('t', 'var * {id: int, name: string})
    >>> s = q.Symbol('name')
    >>> subs(s, t)
    `t.name
    >>> s = q.Symbol('amount')
    >>> s
    `amount
    >>> subs(s, t)  # "amount" isn't a field in t
    `amount
    >>> expr = q.List(q.Atom('='), q.Symbol('name'), 1)
    >>> subs(expr, t)
    (=; `t.name; 1)
    """
    return get(q.List(*list(_subs(expr, t))))


def get(x):
    """Get a q atom from a single element list or return the list.

    Parameters
    ----------
    x : q.Expr
        A Q expression

    Returns
    -------
    r: q.Expr

    Examples
    --------
    >>> s = q.List(q.Atom('='), q.Symbol('t.name'), q.Symbol('Alice'))
    >>> s
    (=; `t.name; `Alice)
    >>> get(s)
    (=; `t.name; `Alice)
    >>> s = q.List(q.Symbol('t.name'))
    >>> get(s)
    `t.name
    """
    try:
        if len(x) == 1:
            return x[0]
    except TypeError:  # our input has no notion of length
        return x
    return x


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
    result = compute_up(expr._child, data, **kwargs)
    return q.List(unops.get(expr.symbol, expr.symbol), result)


@dispatch(Field, q.Expr)
def compute_up(expr, data, **kwargs):
    result = compute_up(expr._child, data, **kwargs)
    try:
        return result[expr._name]
    except TypeError:
        return result


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


@dispatch(Join, q.Expr, q.Expr)
def compute_up(expr, lhs, rhs, **kwargs):
    raise NotImplementedError()


@dispatch(Sort, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List(q.Atom('xasc' if expr.ascending else 'xdesc'),
                  q.List(q.Symbol(expr._key)),
                  compute_up(expr._child, data, **kwargs))


@dispatch(Join, QTable, QTable)
def compute_up(expr, lhs, rhs, **kwargs):
    return compute_up(expr, q.Symbol(lhs.tablename), q.Symbol(rhs.tablename),
                      **kwargs)


@dispatch(Join, q.Expr, dict)
def post_compute(expr, data, scope):
    raise NotImplementedError()


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
    table = first(scope.values())
    return table.engine.eval('eval [%s]' % data)


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


@dispatch(QTable)
def discover(t):
    return tables(t.engine)[t.tablename].dshape
