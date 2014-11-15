import pytest
pytest.importorskip('blaze')
from kdbpy.compute.qtable import tables


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
