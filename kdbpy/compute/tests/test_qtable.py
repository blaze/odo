import pytest
pytest.importorskip('blaze')
from kdbpy.compute.qtable import tables, iskeyed, keys


def test_columns(q, rq, sq):
    assert q.columns == ['name', 'id', 'amount', 'when', 'on']
    assert rq.columns == ['name', 'tax', 'street']
    assert sq.columns == ['name', 'jobcode', 'tree', 'alias']


def test_repr(q):
    expected = """
QTable(tablename='t',
       dshape='var * { name : string, id : int64, amount : float64, when : datetime, on : date }')"""
    assert repr(q) == expected.strip()


def test_tables(kdb, rt, st, t):
    d = tables(kdb)
    keys = set(d.keys())
    assert keys == set(['kt', 'rt', 'st', 't'])
    for s in rt, st, t:
        assert s.isidentical(d[s._name])


def test_tables_repr(kdb):
    assert repr(tables(kdb))


def test_tablename(q, rq, sq):
    assert q.tablename == 't'
    assert rq.tablename == 'rt'
    assert sq.tablename == 'st'


def test_iskeyed(rq, sq, q):
    assert iskeyed(rq)
    assert iskeyed(sq)
    assert not iskeyed(q)


def test_keys(ktq, rq, sq, q):
    rk = keys(rq)
    assert rk == ['name']

    sk = keys(sq)
    assert sk == ['name']

    qk = keys(q)
    assert qk == []

    kqk = keys(ktq)
    assert kqk == ['house', 'id']
