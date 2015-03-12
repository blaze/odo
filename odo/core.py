from __future__ import absolute_import, division, print_function

import networkx as nx
from datashape import discover
from .utils import expand_tuples, ignoring
from contextlib import contextmanager


ooc_types = set()  # Out-of-Core types


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

    def path(self, *args, **kwargs):
        return path(self.graph, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        return _transform(self.graph, *args, **kwargs)


def _transform(graph, target, source, excluded_edges=None, ooc_types=ooc_types,
               **kwargs):
    """ Transform source to target type using graph of transformations """
    x = source
    excluded_edges = excluded_edges or set()
    with ignoring(NotImplementedError):
        if 'dshape' not in kwargs:
            kwargs['dshape'] = discover(x)
    pth = path(graph, type(source), target,
               excluded_edges=excluded_edges,
               ooc_types=ooc_types)
    try:
        for (A, B, f) in pth:
            x = f(x, excluded_edges=excluded_edges, **kwargs)
        return x
    except NotImplementedError as e:
        if kwargs.get('raise_on_errors'):
            raise
        print("Failed on %s -> %s. Working around" % (A.__name__, B.__name__))
        print("Error message:\n%s" % e)
        new_exclusions = excluded_edges | set([(A, B)])
        return _transform(graph, target, source, excluded_edges=new_exclusions,
                          **kwargs)


def path(graph, source, target, excluded_edges=None, ooc_types=ooc_types):
    """ Path of functions between two types """
    if not isinstance(source, type):
        source = type(source)
    if not isinstance(target, type):
        target = type(target)

    if source not in graph:
        for cls in valid_subclasses:
            if issubclass(source, cls):
                source = cls
                break

    # If both source and target are Out-Of-Core types then restrict ourselves
    # to the graph of out-of-core types
    if ooc_types:
        oocs = tuple(ooc_types)
        if issubclass(source, oocs) and issubclass(target, oocs):
            graph = graph.subgraph([n for n in graph.nodes()
                                    if issubclass(n, oocs)])
    with without_edges(graph, excluded_edges) as g:
        pth = nx.shortest_path(g, source=source, target=target, weight='cost')
        result = [(src, tgt, graph.edge[src][tgt]['func'])
                  for src, tgt in zip(pth, pth[1:])]
    return result


# Catch-all subclasses
from collections import Iterator
import numpy as np
valid_subclasses = [Iterator, np.ndarray]


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
