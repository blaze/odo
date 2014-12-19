from blaze import discover
from datashape import dshape
from kdbpy.compute.qtable import tables


def test_columns(q):
    assert q.columns == ['name', 'id', 'amount', 'when', 'on']


def test_repr(q):
    expected = """
QTable(tablename='t',
       dshape='var * { name : string, id : int64, amount : float64, when : datetime, on : date }')"""
    assert repr(q) == expected.strip()


def test_tables(kdb, rt, st, t):
    d = tables(kdb)
    keys = set(d.keys())
    assert set(['kt', 'rt', 'st', 't']).issubset(keys)
    for s in rt, st, t:
        assert s.isidentical(d[s._name])


def test_tables_repr(kdb):
    r = repr(tables(kdb))
    assert r.startswith('Tables({')
    assert r.endswith('})')


def test_tablename(q, rq, sq):
    assert q.tablename == 't'
    assert rq.tablename == 'rt'
    assert sq.tablename == 'st'


def test_keys(ktq, rq, sq, q):
    assert rq.keys == ['name']
    assert sq.keys == ['name']
    assert q.keys == []
    assert ktq.keys == ['house', 'id']


def test_discover_kq(kq, ktq, rq, sq, q):
    result = discover(kq)
    expected = dshape("""{
  dates: var * {account: string, date: date},
  kt: var * {house: string, id: int64, amount: float64},
  prices: var * {account: string, amount: float64},
  rt: var * {name: string, tax: float64, street: string},
  st: var * {name: string, jobcode: int64, tree: string, alias: string},
  t: var * {name: string, id: int64, amount: float64, when: datetime, on: date}
}""")
    assert_db_subset(expected, result)


def assert_db_subset(subset, db):
    inside = set(zip(subset.measure.names, subset.measure.types))
    dbset = set(zip(db.measure.names, db.measure.types))
    assert inside < dbset
