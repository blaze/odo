from .expr import Bar
from . import q
from .core import dispatch


@dispatch(Bar, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    return q.List('xbar', expr.n, child)
