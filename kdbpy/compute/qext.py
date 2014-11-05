from .expr import bar
from .. import q
from .core import dispatch
import blaze as bz


@dispatch(bar, q.Expr)
def compute_up(expr, data, **kwargs):
    child = compute_up(expr._child, data, **kwargs)
    return q.List('xbar', expr.n, child)


@dispatch(bar, q.Expr, dict)
def post_compute(expr, data, d):
    f = post_compute.dispatch(bz.expr.Field, type(data), type(d))
    return f(expr, data, d)
