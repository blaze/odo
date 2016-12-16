from __future__ import absolute_import, division, print_function

from collections import namedtuple, Iterator
from contextlib import contextmanager
from warnings import warn

from datashape import discover
import networkx as nx
import numpy as np
from toolz import concatv

from .compatibility import map
from .utils import expand_tuples, ignoring


ooc_types = set()  # Out-of-Core types


class FailedConversionWarning(UserWarning):
    def __init__(self, src, dest, exc):
        self.src = src
        self.dest = dest
        self.exc = exc

    def __str__(self):
        return 'Failed on %s -> %s. Working around\nError message:\n%s' % (
            self.src.__name__, self.dest.__name__, self.exc,
        )


class IterProxy(object):
    """An proxy to another iterator to support swapping the underlying stream
    mid-iteration.

    Parameters
    ----------
    it : iterable
        The iterable to proxy.

    Attributes
    ----------
    it : iterable
        The iterable being proxied. This can be reassigned to change the
        underlying stream.
    """
    def __init__(self, it):
        self._it = iter(it)

    def __next__(self):
        return next(self.it)
    next = __next__  # py2 compat

    def __iter__(self):
        return self

    @property
    def it(self):
        return self._it

    @it.setter
    def it(self, value):
        self._it = iter(value)


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
    # take a copy so we can mutate without affecting the input
    excluded_edges = (excluded_edges.copy()
                      if excluded_edges is not None else
                      set())

    with ignoring(NotImplementedError):
        if 'dshape' not in kwargs or kwargs['dshape'] is None:
            kwargs['dshape'] = discover(source)

    pth = path(graph, type(source), target,
               excluded_edges=excluded_edges,
               ooc_types=ooc_types)

    x = source
    path_proxy = IterProxy(pth)
    for convert_from, convert_to, f, cost in path_proxy:
        try:
            x = f(x, excluded_edges=excluded_edges, **kwargs)
        except NotImplementedError as e:
            if kwargs.get('raise_on_errors'):
                raise
            warn(FailedConversionWarning(convert_from, convert_to, e))

            # exclude the broken edge
            excluded_edges |= {(convert_from, convert_to)}

            # compute the path from `source` to `target` excluding
            # the edge that broke
            fresh_path = list(path(graph, type(source), target,
                                   excluded_edges=excluded_edges,
                                   ooc_types=ooc_types))
            fresh_path_cost = path_cost(fresh_path)

            # compute the path from the current `convert_from` type
            # to the `target`
            try:
                greedy_path = list(path(graph, convert_from, target,
                                        excluded_edges=excluded_edges,
                                        ooc_types=ooc_types))
            except nx.exception.NetworkXNoPath:
                greedy_path_cost = np.inf
            else:
                greedy_path_cost = path_cost(greedy_path)

            if fresh_path_cost < greedy_path_cost:
                # it is faster to start over from `source` with a new path
                x = source
                pth = fresh_path
            else:
                # it is faster to work around our broken edge from our
                # current location
                pth = greedy_path

            path_proxy.it = pth

    return x


PathPart = namedtuple('PathPart', 'convert_from convert_to func cost')
_virtual_superclasses = (Iterator,)


def path(graph, source, target, excluded_edges=None, ooc_types=ooc_types):
    """ Path of functions between two types """
    if not isinstance(source, type):
        source = type(source)
    if not isinstance(target, type):
        target = type(target)

    for cls in concatv(source.mro(), _virtual_superclasses):
        if cls in graph:
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
        edge = graph.edge

        def path_part(src, tgt):
            node = edge[src][tgt]
            return PathPart(src, tgt, node['func'], node['cost'])

        return map(path_part, pth, pth[1:])


def path_cost(path):
    """Calculate the total cost of a path.
    """
    return sum(p.cost for p in path)


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
