import datashape
from datashape.predicates import iscollection, isnumeric
from blaze.expr import broadcast, Expr
from blaze.expr.expressions import schema_method_list, dshape_method_list


__all__ = ['Bar']


class Bar(Expr):
    __slots__ = '_child', 'n'

    @property
    def dshape(self):
        return datashape.DataShape(datashape.Var(), datashape.int64)


def bar(expr, n):
    return broadcast(Bar, expr, n)


schema_method_list.extend([(isnumeric, set([bar]))])

dshape_method_list.extend([
    (lambda x: iscollection(x) and isnumeric(x), set([bar]))
])
