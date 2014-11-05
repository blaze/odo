import datashape
from datashape import var
from datashape.predicates import iscollection, isnumeric, isdatelike
from blaze.expr import Expr
from blaze.expr.expressions import schema_method_list, dshape_method_list


__all__ = ['bar']


class bar(Expr):
    __slots__ = '_child', 'n'

    @property
    def dshape(self):
        if isnumeric(self._child.schema):
            return var * datashape.int64
        return var * self._child.schema

    @property
    def fields(self):
        return self._child.fields

    @property
    def _name(self):
        return self._child._name


schema_method_list.extend([
    (lambda x: isnumeric(x) or isdatelike(x), set([bar]))
])

dshape_method_list.extend([
    (lambda x: iscollection(x) and (isnumeric(x) or isdatelike(x)), set([bar]))
])
