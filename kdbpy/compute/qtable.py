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
    return dict(syms)


def qp(t):
    t = getattr(t, 'data', t)
    return t.engine.eval('.Q.qp[%s]' % t.tablename).item()


def ispartitioned(t):
    return qp(t) is True


def issplayed(t):
    return qp(t) is False


def isstandard(t):
    return qp(t) is 0


class QTable(object):
    def __init__(self, uri, name=None, columns=None, dshape=None, schema=None):
        self.uri = uri
        self.tablename = name
        self.params = sa.engine.url.make_url(self.uri)
        cred = get_credentials(username=self.params.username,
                               password=self.params.password,
                               host=self.params.host,
                               port=self.params.port)
        self.engine = KQ(cred, start=True)
        self._dshape = dshape or discover(self)
        self.columns = columns or self.engine.eval('cols[%s]' %
                                                   self.tablename).tolist()
        self.schema = schema or self._dshape.measure

    @property
    def dshape(self):
        return self._dshape

    def __repr__(self):
        return ('{0.__class__.__name__}(tablename={0.tablename!r}, '
                'dshape={1!r})'.format(self, str(self.dshape)))


@dispatch(QTable)
def discover(t):
    return tables(t.engine)[t.tablename].dshape
