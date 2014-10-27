import sqlalchemy as sa
from ..kdb import KQ, get_credentials
from datashape import DataShape, Var, Record
from blaze import Symbol, discover
from toolz import second


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
          'u': 'time',  # q minute
          'v': 'time',  # q second
          't': 'time'}


def tables(kdb):
    names = kdb.eval('tables `.')
    metadata = kdb.eval('meta each tables `.')

    # t is the type column in Q
    syms = []
    for name, metatable in zip(names, metadata):
        types = metatable.t
        columns = metatable.index
        ds = DataShape(Var(), Record(list(zip(columns,
                                              [qtypes[t] for t in types]))))
        syms.append((name, Symbol(name, ds)))
    return dict(syms)


class QTable(object):
    def __init__(self, uri, engine=None, name=None, columns=None, dshape=None,
                 schema=None):
        self.uri = uri
        self.tablename = name
        self.params = sa.engine.url.make_url(self.uri)
        cred = get_credentials(username=self.params.username,
                               password=self.params.password,
                               host=self.params.host,
                               port=self.params.port)
        self.engine = engine or KQ(cred, start=True)
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

