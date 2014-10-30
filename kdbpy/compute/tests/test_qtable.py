from kdbpy.compute.qtable import tables


def test_columns(q, rq, sq):
    assert q.columns == ['name', 'id', 'amount']
    assert rq.columns == ['name', 'tax', 'street']
    assert sq.columns == ['name', 'jobcode', 'tree']


def test_repr(q):
    expected = ("QTable(tablename='t', "
                "dshape='var * "
                "{ name : string, id : int64, amount : float64 }')")
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
