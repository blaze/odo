from blaze.expr import Expr
from .core import dispatch
from kdbpy.compute.qtable import QTable


class QPartitionedTable(QTable):
    pass


@dispatch(Expr, q.Expr, bool)
def optimize(expr, data, partitioned):
    pass
