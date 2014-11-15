from collections import OrderedDict
from kdbpy import KQ
from datashape import Record, var
from blaze import Symbol, discover
from blaze.dispatch import dispatch
from kdbpy.util import PrettyMixin, parse_connection_string
from kdbpy import q


qtypes = {'b': 'bool',
          'x': 'int8',
          'h': 'int16',
          'i': 'int32',
          'j': 'int64',
          'e': 'float32',
          'f': 'float64',
          'c': 'string',  # q char
          'C': 'string',  # q char
          's': 'string',  # q symbol
          'm': 'date',  # q month
          'd': 'date',
          'z': 'datetime',
          'p': 'datetime',  # q timestamp
          'u': 'timedelta[unit="m"]',  # q minute
          'v': 'timedelta[unit="s"]',  # q second
          't': 'timedelta'}


class Tables(PrettyMixin, OrderedDict):
    def __init__(self, *args, **kwargs):
        super(Tables, self).__init__(*args, **kwargs)

    def _repr_pretty_(self, p, cycle):
        assert not cycle, 'cycles not allowed'
        with p.group(4, '%s({' % type(self).__name__, '})'):
            for idx, (k, v) in enumerate(self.items()):
                if idx:
                    p.text(',')
                    p.breakable()
                p.pretty(k)
                p.text(': ')
                p.pretty(v)


def tables(kdb):
    names = kdb.tables.name
    metadata = kdb.eval(r'meta each value "\\a"')

    # t is the type column of the result of "meta `t" in q
    syms = []
    for name, meta in zip(names, metadata):
        types = meta.t
        columns = meta.index
        ds = var * Record(list(zip(columns, [qtypes[t] for t in types])))
        syms.append((name, Symbol(name, ds)))
    return Tables(syms)


def qp(t):
    t = getattr(t, 'data', t)
    return t.engine.eval('.Q.qp[%s]' % t.tablename).item()


def ispartitioned(t):
    return qp(t) is True


def issplayed(t):
    return qp(t) is False


def isstandard(t):
    return int(qp(t)) is 0


class QTable(PrettyMixin):
    def __init__(self, uri, tablename=None, columns=None, dshape=None,
                 schema=None):
        self.uri = uri
        self.tablename = tablename
        self.engine = KQ(parse_connection_string(self.uri), start=True)
        self.dshape = dshape or discover(self)
        self.columns = columns or self.engine.eval('cols[%s]' %
                                                   self.tablename).tolist()
        self.schema = schema or self.dshape.measure

    def eval(self, expr, *args, **kwargs):
        return self.engine.eval('eval [%s]' % expr, *args, **kwargs)

    def _repr_pretty_(self, p, cycle):
        assert not cycle, 'cycles not allowed'
        name = type(self).__name__
        with p.group(len(name) + 1, '%s(' % name, ')'):
            p.text('tablename=')
            p.pretty(self.tablename)
            p.text(',')
            p.breakable()
            p.text('dshape=')
            p.pretty(str(self.dshape))

    @property
    def _qsymbol(self):
        return q.Symbol(self.tablename)

    @property
    def _symbol(self):
        return Symbol(self.tablename, self.dshape)

    @property
    def iskeyed(self):
        sym = self._qsymbol
        return self.eval(q.and_(q.istable(sym), q.isdict(sym)))

    @property
    def keys(self):
        if self.iskeyed:
            expr = q.List('cols', q.List('key', self._qsymbol))
            return self.eval(expr).tolist()
        return []


@dispatch(QTable)
def discover(t):
    return tables(t.engine)[t.tablename].dshape
