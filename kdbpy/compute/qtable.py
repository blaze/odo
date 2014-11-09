import IPython
from collections import OrderedDict
import sqlalchemy as sa
from ..kdb import KQ, get_credentials
from datashape import Record, var
from blaze import Symbol, discover
from blaze.dispatch import dispatch


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


class Tables(OrderedDict):
    def __init__(self, *args, **kwargs):
        super(Tables, self).__init__(*args, **kwargs)
        for k, v in self.items():
            if not isinstance(k, basestring):
                raise TypeError('keys must all be strings')
            if not isinstance(v, Symbol):
                raise TypeError('values must all be blaze Symbol instances')

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('%s(...)' % type(self).__name__)
        else:
            with p.group(4, '%s({' % type(self).__name__, '})'):
                for idx, (k, v) in enumerate(self.items()):
                    if idx:
                        p.text(',')
                        p.breakable()
                    p.pretty(k)
                    p.text(': ')
                    p.pretty(v)

    def __repr__(self):
        return IPython.lib.pretty.pretty(self)


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
    return qp(t) is 0


def parse_connection_string(uri):
    params = sa.engine.url.make_url(uri)
    return get_credentials(username=params.username, password=params.password,
                           host=params.host, port=params.port)


class QTable(object):
    def __init__(self, uri, tablename=None, columns=None, dshape=None,
                 schema=None):
        self.uri = uri
        self.tablename = tablename
        self.engine = KQ(parse_connection_string(self.uri), start=True)
        self.dshape = dshape or discover(self)
        self.columns = columns or self.engine.eval('cols[%s]' %
                                                   self.tablename).tolist()
        self.schema = schema or self.dshape.measure

    def _repr_pretty_(self, p, cycle):
        name = type(self).__name__
        if cycle:
            p.text('%s(...)' % name)
        else:
            with p.group(len(name) + 1, '%s(' % name, ')'):
                p.text('tablename=')
                p.pretty(self.tablename)
                p.text(',')
                p.breakable()
                p.text('dshape=')
                p.pretty(str(self.dshape))

    def __repr__(self):
        return IPython.lib.pretty.pretty(self)


@dispatch(QTable)
def discover(t):
    return tables(t.engine)[t.tablename].dshape
