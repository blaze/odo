from into.graph import *

def test_graph_core():
    g = Graph()
    g.add('a', 'b', cost=10)
    g.add('b', 'c', cost=10)
    g.add('a', 'c', cost=30)

    assert first(g.paths()) == ('a', 'b', 'c')
