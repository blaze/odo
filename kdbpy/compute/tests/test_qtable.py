import pytest
pytest.importorskip('blaze')
from kdbpy.compute.qtable import tables


def test_columns(q, rq, sq):
    assert q.columns == ['name', 'id', 'amount', 'when', 'on']
    assert rq.columns == ['name', 'tax', 'street']
    assert sq.columns == ['name', 'jobcode', 'tree', 'alias']


def test_repr(q):
    expected = ("QTable(tablename='t', "
                "dshape='var * "
                "{ name : string, id : int64, amount : float64, when : datetime, on : date }')")
    assert repr(q) == expected


def test_tables(kdb, rt, st, t):
    d = tables(kdb)
    keys = set(d.keys())
    assert keys == set(['rt', 'st', 't'])
    for s in rt, st, t:
        assert s.isidentical(d[s._name])


def test_tablename(q, rq, sq):
    assert q.tablename == 't'
    assert rq.tablename == 'rt'
    assert sq.tablename == 'st'
