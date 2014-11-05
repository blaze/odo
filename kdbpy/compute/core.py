"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division

import numbers

import pandas as pd

from toolz.compatibility import zip
from toolz import map, identity, first, second

from blaze import resource

from blaze.dispatch import dispatch

from blaze.expr import Symbol, Projection, Selection, Field, FloorDiv
from blaze.expr import BinOp, UnaryOp, Expr, Reduction, By, Join, Head, Sort
from blaze.expr import nelements, Slice, Distinct, Summary, Relational
from blaze.expr import DateTime, Millisecond, Microsecond
from blaze.expr.datetime import Minute

from datashape.predicates import isrecord

from .. import q
from .qtable import QTable, tables


binops = {
    '!=': q.Atom('<>'),
    '/': q.Atom('%'),
    '%': q.Atom('mod'),
    '**': q.Atom('xexp'),
    '==': q.Atom('=')
}


unops = {
    '~': q.Atom('~:'),
    '-': q.Atom('-:')
}


reductions = {
    'mean': 'avg',
    'std': 'dev',
}


qdatetimes = {
    'day': 'dd',
    'month': 'mm',
    'hour': 'hh',
    'second': 'ss',
}


def get_wrapper(expr, types=(basestring,)):
    return q.List if isinstance(expr, types) else identity


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
    if len(x) == 1:
        return x[0]
    return x


def desubs(expr, t):
    """Remove a particular table `t` from an expression.

    TODO
    ----
    Is looking at the name of the table sufficient?

    Examples
    --------
    >>> import blaze as bz
    >>> t = bz.Symbol('t', 'var * {name: string, amount: float64}')
    >>> s = q.Symbol('t.name')
    >>> desubs(s, t)
    `name
    >>> s = q.List(q.Atom('first'), q.Symbol('t.name'))
    >>> s
    (first; `t.name)
    >>> desubs(s, t)
    (first; `name)
    """
    return get(q.List(*list(_desubs(expr, t))))


def compute_atom(atom, symbol):
    if '.' in atom.s and atom.s.startswith(symbol._name):
        return type(atom)(second(atom.s.rsplit('.', 1)))
    return atom


def _desubs(expr, t):
    if isinstance(expr, q.Atom):
        yield compute_atom(expr, t)
    elif isinstance(expr, (basestring, numbers.Number, q.Bool)):
        yield expr
    else:
        for sube in expr:
            if isinstance(sube, q.Atom):
                yield compute_atom(sube, t)
            elif isinstance(sube, q.List):
                yield q.List(*(desubs(s, t) for s in sube))
            elif isinstance(sube, q.Dict):
                yield q.Dict([(desubs(k, t), desubs(v, t))
                              for k, v in sube.items()])
            else:  # isinstance(sube, (basestring, numbers.Number, q.Bool)):
                yield sube


@dispatch(basestring, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.Symbol(expr)


@dispatch(Projection, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    fields = list(map(q.Symbol, expr.fields))
    return q.List('?', child, q.List(), q.Bool(), q.Dict(list(zip(fields,
                                                                  fields))))


@dispatch(Symbol, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.Symbol(expr._name)


@dispatch(numbers.Number, q.Expr)
def compute_up(expr, data, **kwargs):
    return expr


@dispatch((BinOp, Relational), q.Expr)
def compute_up(expr, data, **kwargs):
    symbol = expr.symbol
    op = binops.get(symbol, symbol)
    lhs, rhs = expr.lhs, expr.rhs
    lwrap, rwrap = get_wrapper(lhs), get_wrapper(rhs)
    lhs = lwrap(compute_up(lhs, data, **kwargs))
    rhs = rwrap(compute_up(rhs, data, **kwargs))
    return q.List(op, lhs, rhs)


@dispatch((BinOp, Relational), q.Expr, q.Expr)
def compute_up(expr, lhs, rhs, **kwargs):
    symbol = expr.symbol
    op = binops.get(symbol, symbol)
    return q.List(op, lhs, rhs)


@dispatch(UnaryOp, q.Expr)
def compute_up(expr, data, **kwargs):
    symbol = expr.symbol
    result = compute_up(expr._child, data, **kwargs)
    return q.List(unops.get(symbol, symbol), result)


@dispatch(FloorDiv, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List('_:', compute_up(expr.lhs / expr.rhs, data, **kwargs))


@dispatch(Field, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    return child[expr._name]


@dispatch(Field, q.Expr, dict)
def post_compute(expr, data, scope):
    table = first(scope.values())
    leaf = expr._leaves()[0]
    subsed = expr._subs({leaf: Symbol(table.tablename, leaf.dshape)})
    final_expr = compute_up(subsed, data, scope=scope)
    result = table.engine.eval('eval [%s]' % final_expr).squeeze()
    result.name = expr._name
    return result


@dispatch(Selection, q.Expr)
def compute_up(expr, data, **kwargs):
    # template: ?[select, list of predicates, by, aggregations]
    predicate = compute_up(expr.predicate, data, **kwargs)
    select = compute_up(expr._child, data, **kwargs)
    return q.List('?', select, q.List(q.List(predicate)), q.Bool(), q.List())


@dispatch(DateTime, q.Expr)
def compute_up(expr, data, **kwargs):
    attr = expr.attr
    attr = qdatetimes.get(attr, attr)
    return q.Symbol('.'.join((expr._child._child._name, expr._child._name,
                              attr)))


@dispatch(Microsecond, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List('_:',
                  q.List('%',
                         q.List('mod',
                                q.List('$', q.List(q.Symbol('long')),
                                       q.Symbol('%s.%s' %
                                                (expr._child._child._name,
                                                 expr._child._name))),
                                int(1e9)),
                         int(1e3)))


@dispatch(Millisecond, q.Expr)
def compute_up(expr, data, **kwargs):
    return compute_up(expr._child.microsecond // 1000, data, **kwargs)


@dispatch(Minute, q.Expr)
def compute_up(expr, data, **kwargs):
    # q has mm for time types and mm for datetime and date types, this makes -1
    # amount of sense, so we bypass that and compute it our damn selves using
    # (`long$expr.minute) mod 60
    return q.List('mod', q.List('$', q.List(q.Symbol('long')),
                                q.Symbol(str(expr))), 60)


@dispatch(Reduction, q.Expr)
def compute_up(expr, data, **kwargs):
    symbol = expr.symbol
    return q.List(reductions.get(symbol, symbol),
                  compute_up(expr._child, data, **kwargs))


@dispatch(Join, q.Expr, q.Expr)
def compute_up(expr, lhs, rhs, **kwargs):
    if expr.how != 'inner':
        raise NotImplementedError('only inner joins supported')
    if expr._on_left != expr._on_right:
        raise NotImplementedError('can only join on same named columns')
    return q.List('ej', q.List(q.Symbol(expr._on_left)), lhs, rhs)


@dispatch(Sort, q.Expr)
def compute_up(expr, data, **kwargs):
    sort_func = q.Atom('xasc' if expr.ascending else 'xdesc')
    key = q.List(q.Symbol(expr._key))
    child = compute_up(expr._child, data, **kwargs)
    return q.List(sort_func, key, child)


@dispatch(Join, QTable, QTable)
def compute_up(expr, lhs, rhs, **kwargs):
    return compute_up(expr, q.Symbol(lhs.tablename), q.Symbol(rhs.tablename),
                      **kwargs)


@dispatch(Summary, q.Expr)
def compute_up(expr, data, **kwargs):
    ops = [compute_up(op, data, **kwargs) for op in expr.values]
    names = expr.names
    child = compute_up(expr._child, data, **kwargs)
    return q.List('?', child, q.List(), q.Bool(),
                  q.Dict(list(zip(map(q.Symbol, names), ops))))


@dispatch(By, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    grouper = compute_up(expr.grouper, data, **kwargs)
    grouper = q.Dict([(q.Symbol(expr.grouper._name), grouper)])
    reducer = compute_up(expr.apply, data, **kwargs)

    if not isinstance(expr.apply, Summary):
        reducer = q.Dict([(q.Symbol(expr.apply._name), reducer)])
    else:
        # we only need the reduction dictionary from the result of a summary
        # parse
        reducer = reducer[-1]

    qexpr = q.List('?', child, q.List(), grouper, reducer)
    table = first(kwargs['scope'].keys())

    # TODO: fix this using blaze core functions
    return desubs(qexpr, table)


@dispatch(nelements, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    axis_funcs = {0: q.List('#:', child),
                  1: q.List('#:', q.List('cols', child))}
    return q.List('prd', *[axis_funcs[i] for i in expr.axis or expr.ndim])


@dispatch(Head, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)

    final_index = q.List('&', expr.n, compute_up(expr._child.nelements(axis=0),
                                                 data, **kwargs))

    # TODO: generate different code if we are a partitioned table
    # we need to use the global index to do this

    # q repeats if the N of take is larger than the number of rows, so we
    # need to get the min of the number of rows and the requested N from the
    # Head expression
    return q.List('#', final_index, child)


@dispatch(Expr, q.Expr, dict)
def post_compute(expr, data, scope):
    # never a Data object
    tables = set(x for x in scope.values() if isinstance(x, QTable))
    assert len(tables) == 1
    table = first(tables)
    leaf = expr._leaves()[0]

    # do this in optimize
    subsed = expr._subs({leaf: Symbol(table.tablename, leaf.dshape)})
    final_expr = compute_up(subsed, data, scope=scope)
    return table.engine.eval('eval [%s]' % final_expr)


@dispatch(Join, q.Expr, dict)
def post_compute(expr, data, scope):
    # never a Data object
    tables = set(x for x in scope.values() if isinstance(x, QTable))
    table = first(tables)
    # leaf = expr._leaves()[0]

    # do this in optimize
    # subsed = expr._subs({leaf: Symbol(table.tablename, leaf.dshape)})
    # final_expr = compute_up(subsed, data, scope=scope)
    final_expr = data
    return table.engine.eval('eval [%s]' % final_expr)


@dispatch(Slice, q.Expr)
def compute_up(expr, data, **kwargs):
    """Slice expressions from Python to Q.

    Notes
    -----
    ``sublist`` is actually defined in K land so we have to jump through hoops
    to actually evaluate it properly.

    In Q::

        r: X sublist Y
        3 sublist 1 2 3 4 5 = 1 2 3
        1 3 sublist 1 2 3 4 5 = 2 3 4
        x = [1, 2, 3, 4, 5]
        Y[2:5] == 2 3 sublist Y
        Y[a:b] == a (b - a) sublist Y
    """
    assert len(expr.index) == 1, 'only single slice allowed'
    index, = expr.index
    nrows = compute_up(expr._child.nelements(axis=0), data, **kwargs)
    child = compute_up(expr._child, data, **kwargs)

    if isinstance(index, numbers.Integral):
        if index < 0:
            index = q.List('+', index, nrows)

        qexpr = q.List(child, index)

        if not isrecord(expr.dshape):
            return qexpr
        return q.List(',:', qexpr)

    # assuming a slice here
    start = index.start or 0
    stop = index.stop or nrows
    return q.List('sublist', q.List('enlist', start,
                                    q.List('![-6]', q.List('-', stop, start))),
                  child)


@dispatch(Distinct, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List(q.Atom('?:'), compute_up(expr._child, data, **kwargs))


@dispatch(Expr, QTable)
def compute_up(expr, data, **kwargs):
    return compute_up(expr, q.Symbol(data.tablename), **kwargs)


@dispatch(Expr, QTable)
def compute_down(expr, data, **kwargs):
    return compute_down(expr, q.Symbol(data.tablename), **kwargs)


@dispatch(QTable)
def discover(t):
    return tables(t.engine)[t.tablename].dshape


@resource.register('kdb://.+', priority=13)
def resource_kdb(uri, name, **kwargs):
    return QTable(uri, name=name, **kwargs)


@dispatch(pd.DataFrame, QTable)
def into(_, t, **kwargs):
    return t.engine.eval(t.tablename)
