from __future__ import absolute_import, division, print_function

import networkx as nx
from blaze import discover
from .utils import expand_tuples


class NetworkDispatcher(object):
    def __init__(self, name):
        self.name = name
        self.graph = nx.DiGraph()

    def register(self, a, b, cost=1.0):
        sigs = expand_tuples([a, b])
        def _(func):
            for a, b in sigs:
                self.graph.add_edge(b, a, cost=1.0, func=func)
            return func
        return _

    def to_pydot(self):
        dg = nx.DiGraph()
        for a, b in self.graph.edges():
            cost = self.graph.edge[a][b]['cost']
            dg.add_edge(a.__name__, b.__name__, cost=cost)
        return nx.to_pydot(dg)


    def path(self, a, b):
        path = nx.shortest_path(self.graph, source=a, target=b, weight='cost')
        return [self.graph.edge[a][b]['func'] for a, b in zip(path, path[1:])]

    def func(self, a, b):
        def transform(x, **kwargs):
            if 'dshape' not in kwargs:
                kwargs['dshape'] = discover(x)
            x
            for f in self.path(a, b):
                oldx = x
                x = f(x, **kwargs)
            return x
        return transform

    def __call__(self, a, b, **kwargs):
        func = self.func(type(b), a)
        return func(b, **kwargs)
