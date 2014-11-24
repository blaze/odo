from .expr import bar
from .. import q
from .core import dispatch


@dispatch(bar, q.Expr)
def compute_up(expr, data, **kwargs):
    return q.List('xbar', expr.n, data)
