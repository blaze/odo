"""
A blaze backend that generates Q code
"""

from __future__ import absolute_import, print_function, division

import numbers

from . import q
from .qtable import QTable, tables, ispartitioned

import qpython.qcollection

import pandas as pd

from blaze.dispatch import dispatch

import blaze as bz
from blaze import resource
from blaze.expr import Symbol, Projection, Broadcast, Selection, Field
from blaze.expr import BinOp, UnaryOp, Expr, Reduction, By, Join, Head, Sort
from blaze.expr import nrows, Slice, Distinct, Summary

from datashape.predicates import isrecord

from toolz.curried import map
from toolz.compatibility import zip
from toolz import identity, first, second


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
    sym = q.Symbol(expr._name)
    t = first(kwargs['scope'].keys())
    if t.isidentical(expr) or (t._name == expr._name and
                               t.dshape == expr.dshape):
        return sym
    return subs(sym, sym, q.Symbol('%s.%s' % (t._name, sym.s)))


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


def get_wrapper(expr, types=(basestring,)):
    return q.List if isinstance(expr, types) else identity


def manip(func, *args, **kwargs):
    return get(q.List(*list(func(*args, **kwargs))))


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


def _subs(expr, old, new):
    if isinstance(expr, q.Symbol):
        # TODO: how do we handle multiple tables?
        if '.' not in expr.s:
            yield new if expr == old else expr
        else:
            subsed = [subs(e, old, new).s
                      for e in map(q.Symbol, expr.s.split('.'))]
            r = q.Symbol('.'.join(subsed))
            yield r
    elif isinstance(expr, (numbers.Number, basestring, q.Atom)):
        yield expr
    elif isinstance(expr, (q.List, q.Dict)):
        for sube in expr:
            if isinstance(sube, q.List):
                yield q.List(*(x for x in _subs(sube, old, new)))
            elif isinstance(sube, q.Atom):
                # recurse back into subs (not _subs) to have it call get
                yield subs(sube, old, new)
            elif isinstance(sube, (basestring, numbers.Number, q.Bool)):
                yield sube
            elif isinstance(sube, q.Dict):
                yield q.Dict([(subs(x, old, new), subs(y, old, new))
                              for x, y in sube.items()])
            else:
                raise ValueError('unknown type for substitution '
                                 '{!r}'.format(type(sube).__name__))


def subs(expr, old, new):
    """Substitute a parent table name for fields in a broadcast expression.

    Parameters
    ----------
    expr : q.Expr, q.Atom, int, basestring
        The expression in which to substitute the parent table
    old
    new

    Returns
    -------
    expression : q.Expr

    Examples
    --------
    >>> from blaze import Symbol
    >>> t = Symbol('t', 'var * {id: int, name: string}')
    >>> s = q.Symbol('name')
    >>> subs(s, s, q.Symbol('t.%s' % s.s))
    `t.name
    >>> s = q.Symbol('name')
    >>> s
    `name
    >>> expr = q.List(q.Atom('='), s, 1)
    >>> subs(expr, s, q.Symbol('%s.%s' % (t._name, s.s)))
    (=; `t.name; 1)
    """
    return manip(_subs, expr, old, new)


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
    return manip(_desubs, expr, t)


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
            elif isinstance(sube, (basestring, numbers.Number, q.Bool)):
                yield sube
            else:
                raise NotImplementedError()


@dispatch(numbers.Number, q.Expr)
def compute_up(expr, data, **kwargs):
    return expr


@dispatch(BinOp, q.Expr)
def compute_up(expr, data, **kwargs):
    symbol = expr.symbol
    op = binops.get(symbol, symbol)
    lhs, rhs = expr.lhs, expr.rhs
    lwrap, rwrap = get_wrapper(lhs), get_wrapper(rhs)
    lhs = lwrap(compute_up(lhs, data, **kwargs))
    rhs = rwrap(compute_up(rhs, data, **kwargs))
    return q.List(op, lhs, rhs)


@dispatch(UnaryOp, q.Expr)
def compute_up(expr, data, **kwargs):
    symbol = expr.symbol
    result = compute_up(expr._child, data, **kwargs)
    return q.List(unops.get(symbol, symbol), result)


@dispatch(Field, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    try:
        return child[expr._name]
    except TypeError:
        return child


@dispatch(Field, q.Expr, dict)
def post_compute(expr, data, scope):
    table = first(scope.values())
    final_expr = subs(data, q.Symbol(expr._leaves()[0]._name),
                      q.Symbol(table.tablename))
    result = table.engine.eval('eval [%s]' % final_expr)
    result.name = expr._name
    return result


@dispatch(Broadcast, q.Expr)
def compute_up(expr, data, **kwargs):
    return compute_up(expr._expr, data, **kwargs)


@dispatch(Selection, q.Expr)
def compute_up(expr, data, **kwargs):
    predicate = compute_up(expr.predicate, data, **kwargs)
    return q.List('?', compute_up(expr._child, data, **kwargs),
                  q.List(q.List(predicate)), q.Bool(), q.List())


reductions = {
    'mean': 'avg',
    'std': 'dev',
}


@dispatch(Reduction, q.Expr)
def compute_up(expr, data, **kwargs):
    symbol = expr.symbol
    return q.List(reductions.get(symbol, symbol),
                  compute_up(expr._child, data, **kwargs))


@dispatch(Join, q.Expr, q.Expr)
def compute_up(expr, lhs, rhs, **kwargs):
    raise NotImplementedError()


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
    qexpr = q.List('?', child, q.List(), q.Bool(),
                   q.Dict(list(zip(map(q.Symbol, names), ops))))
    return qexpr


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
    qexpr = desubs(qexpr, table)
    return qexpr


@dispatch(nrows, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List('#:', compute_up(expr._child, data, **kwargs))


@dispatch(Head, q.Expr)
def compute_up(expr, data, **kwargs):
    table = first(kwargs['scope'].values())
    child = compute_up(expr._child, data, **kwargs)

    final_index = q.List('&', expr.n, compute_up(expr._child.nrows(), data,
                                                 **kwargs))

    if ispartitioned(table):
        # generate different code if we are a partitioned table
        # we need to use the global index to do this
        qexpr = q.List('.Q.ind', child, q.List('til', final_index))
    else:
        # q repeats if the N of take is larger than the number of rows, so we
        # need to get the min of the number of rows and the requested N from the
        # Head expression
        qexpr = q.List('#', final_index, child)
    return qexpr


@dispatch(Expr, q.Expr, dict)
def post_compute(expr, data, scope):
    tables = set(x for x in scope.values() if isinstance(x, (bz.Data, QTable)))
    assert len(tables) == 1
    table = first(tables)
    table = getattr(table, 'data', table)
    final_expr = subs(data, q.Symbol(expr._leaves()[0]._name),
                      q.Symbol(table.tablename))
    return table.engine.eval('eval [%s]' % final_expr)


@dispatch(Join, q.Expr, dict)
def post_compute(expr, data, scope):
    raise NotImplementedError()


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
    nrows = compute_up(expr._child.nrows(), data, **kwargs)
    child = compute_up(expr._child, data, **kwargs)

    if isinstance(index, numbers.Integral):
        if index < 0:
            index = q.List('+', index, nrows)

        qexpr = q.List(child, index)

        if isrecord(expr.dshape):
            qexpr = q.List(',:', qexpr)  # +: == flip

    elif isinstance(index, slice):
        start = index.start or 0
        stop = index.stop or nrows
        qexpr = q.List('sublist', q.List('enlist', start,
                                         # eval ~ ![-6] // q is so gross
                                         q.List('![-6]', q.List('-', stop,
                                                                start))),
                       child)
    else:
        raise NotImplementedError('slices of type %r are not implemented for Q '
                                  'expressions' % type(index).__name__)

    return qexpr


@dispatch(Distinct, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List(q.Atom('?:'), compute_up(expr._child, data, **kwargs))


@dispatch(Expr, QTable)
def compute_up(expr, data, **kwargs):
    return compute_up(expr, q.Symbol(data.tablename), **kwargs)


@dispatch(pd.DataFrame, qpython.qcollection.QKeyedTable)
def into(_, tb, **kwargs):
    keys = tb.keys
    names = keys.dtype.names
    index = pd.MultiIndex.from_arrays([keys[name] for name in names],
                                      names=names)
    return pd.DataFrame.from_records(tb.values, index=index, **kwargs)


@dispatch(pd.DataFrame, qpython.qcollection.QTable)
def into(_, tb, **kwargs):
    return pd.DataFrame.from_records(tb.values, **kwargs)


@dispatch(pd.Series, qpython.qcollection.QDictionary)
def into(frame, d, **kwargs):
    return pd.Series(d.values, index=d.keys)


@dispatch(QTable)
def discover(t):
    return tables(t.engine)[t.tablename].dshape


@resource.register('kdb://.+', priority=13)
def resource_kdb(uri, name, **kwargs):
    return QTable(uri, name=name, **kwargs)
