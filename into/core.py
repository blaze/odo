from __future__ import absolute_import, division, print_function

import networkx as nx
from blaze import discover
from .utils import expand_tuples, cls_name
from contextlib import contextmanager


class NetworkDispatcher(object):
    def __init__(self, name):
        self.name = name
        self.graph = nx.DiGraph()

    def register(self, a, b, cost=1.0):
        sigs = expand_tuples([a, b])
        def _(func):
            for a, b in sigs:
                self.graph.add_edge(b, a, cost=cost, func=func)
            return func
        return _

    def path(self, source, target, excluded_edges=None):
        if not isinstance(source, type):
            source = type(source)
        if not isinstance(target, type):
            target = type(target)
        with without_edges(self.graph, excluded_edges) as g:
            path = nx.shortest_path(g, source=source, target=target, weight='cost')
            result = [(source, target, self.graph.edge[source][target]['func'])
                        for source, target in zip(path, path[1:])]
        return result

    def func(self, a, b):
        def transform(x, **kwargs):
            if 'dshape' not in kwargs:
                kwargs['dshape'] = discover(x)
            for (A, B, f) in self.path(a, b):
                oldx = x
                x = f(x, **kwargs)
            return x
        return transform

    def __call__(self, target, source, excluded_edges=None, **kwargs):
        x = source
        excluded_edges = excluded_edges or set()
        if 'dshape' not in kwargs:
            kwargs['dshape'] = discover(x)
        path = self.path(type(source), target, excluded_edges=excluded_edges)
        try:
            for (A, B, f) in path:
                oldx = x
                x = f(x, **kwargs)
            return x
        except:
            print("Failed on %s -> %s. Working around" %
                        (A.__name__,  B.__name__))
            new_exclusions = excluded_edges | set([(A, B)])
            return self(target, source, excluded_edges=new_exclusions, **kwargs)



@contextmanager
def without_edges(g, edges):
    edges = edges or []
    held = dict()
    for a, b in edges:
        held[(a, b)] = g.edge[a][b]
        g.remove_edge(a, b)

    try:
        yield g
    finally:
        for (a, b), kwargs in held.items():
            g.add_edge(a, b, **kwargs)
