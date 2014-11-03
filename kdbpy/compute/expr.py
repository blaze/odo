import datashape
from datashape.predicates import iscollection, isnumeric
from blaze.expr import Expr
from blaze.expr.expressions import schema_method_list, dshape_method_list


__all__ = ['bar']


class bar(Expr):
    __slots__ = '_child', 'n'

    @property
    def dshape(self):
        dim = datashape.Var()
        if isnumeric(self._child.schema):
            return datashape.DataShape(dim, datashape.int64)
        return datashape.DataShape(dim, self._child.measure)

    @property
    def fields(self):
        return self._child.fields


schema_method_list.extend([(isnumeric, set([bar]))])

dshape_method_list.extend([
    (lambda x: iscollection(x) and isnumeric(x), set([bar]))
])
